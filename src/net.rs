use atomic_float::AtomicF64;
use futures::{future::join_all, lock::Mutex, Future};
use reqwest::{Client, ClientBuilder, IntoUrl, Proxy, Request, Response};
use std::{
    convert::identity,
    ops::{DerefMut, Range},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering, AtomicU32},
        Arc,
    },
    time::SystemTime,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::{io::AsyncBufReadExt, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::dbg_lvl;

pub trait NetClientManager {
    fn get(&self, url: &dyn IntoUrl) -> reqwest::Result<Response>;
}

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: u32, // Usually matches kernel id
    pub tor_path: String,
    pub config_path: String,
    pub port: u32,
    pub timeout: f64,
}

#[derive(Debug, Copy, Clone)]
pub struct VanillaClient;

impl VanillaClient {
    pub fn client() -> Option<Client> {
        ClientBuilder::new().build().ok()
    }
}

impl Iterator for VanillaClient {
    type Item = Client;

    fn next(&mut self) -> Option<Self::Item> {
        Self::client()
    }
}

pub struct TorClientIterator<'a> {
    mgr: &'a TorClientManager,
    i: usize,
}

impl<'a> Iterator for TorClientIterator<'a> {
    type Item = Client;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.mgr.clients.is_empty() {
            self.i += 1;
            if self.i >= self.mgr.clients.len() {
                self.i = 0;
            }
            let c = &self.mgr.clients[self.i];
            return Some(c.client.clone());
        }
        None
    }
}

pub struct TorClientManager {
    clients: Vec<TorClient>,
}

pub struct TorClient {
    client_info: ClientInfo,
    client: Client,
    cmd: Option<tokio::process::Child>,
}

impl TorClientManager {
    pub const TOR_DATA_PATH: &'static str = "tor/data";
    pub const TOR_CONFIG_PATH: &'static str = "tor/config";

    pub async fn generate_configs(
        tor_path: String,
        base_config: String,
        port_range: Range<u32>,
        timeout: f64,
    ) -> std::io::Result<Self> {
        let base_cfg = std::fs::read_to_string(&base_config)?;
        let mut client_configs: Vec<ClientInfo> = Vec::with_capacity(port_range.len());

        let working_dir = std::env::current_dir()?;

        std::fs::create_dir_all(Self::TOR_DATA_PATH)?;
        std::fs::create_dir_all(Self::TOR_CONFIG_PATH)?;

        for (i, port) in port_range.enumerate() {
            let data_dir = format!("{}/{}/data_{}", working_dir.to_string_lossy(), Self::TOR_DATA_PATH, i);
            let cfg = format!(
                "{}\nDataDirectory {}\nSocksListenAddress 127.0.0.1:{}\nSocksPort {}",
                base_cfg.clone(),
                data_dir,
                port,
                port
            );

            let config_path = format!(
                "{}/{}/temp_torrc_{}",
                working_dir.to_str().unwrap(),
                Self::TOR_CONFIG_PATH,
                i
            );

            std::fs::write(&config_path, cfg)?;

            client_configs.push(ClientInfo {
                client_id: i as u32,
                tor_path: tor_path.clone(),
                config_path,
                port,
                timeout,
            });
        }
        Ok(Self::new(client_configs.into_iter()).await)
    }

    /// Assume tor circuits are already running and configs exist in their default position.
    /// This saves a huge amount of time over restarting from cold when a small change is made, since booting up the tor circuits takes ages.
    pub fn from_generated_configs(
        tor_path: String,
        port_range: Range<u32>,
        timeout: f64,
    ) -> std::io::Result<Self> {
        let mut client_configs: Vec<ClientInfo> = Vec::with_capacity(port_range.len());
        let working_dir = std::env::current_dir()?;
        for (i, port) in port_range.enumerate() {
            let config_path = format!(
                "{}/{}/temp_torrc_{}",
                working_dir.to_str().unwrap(),
                Self::TOR_CONFIG_PATH,
                i
            );

            client_configs.push(ClientInfo {
                client_id: i as u32,
                tor_path: tor_path.clone(),
                config_path,
                port,
                timeout,
            });
        }
        Ok(Self::new_generated(client_configs.into_iter()))
    }

    pub async fn new(client_configs: impl Iterator<Item = ClientInfo>) -> Self {
        let hs = client_configs
            .into_iter()
            .map(|client_info| Self::load_client(client_info))
            .collect::<Vec<_>>();

        // Launch all clients simultaneously.
        let clients = join_all(hs)
            .await
            .into_iter()
            .filter_map(identity)
            .collect();

        Self { clients }
    }

    fn new_generated(client_configs: impl Iterator<Item = ClientInfo>) -> Self {
        let clients = client_configs
            .into_iter()
            .filter_map(|client_info| Self::existing_client(client_info))
            .collect();
        Self { clients } 
    }

    pub fn iter(&self) -> TorClientIterator<'_> {
        TorClientIterator { i: 0, mgr: &self }
    }

    async fn load_client(client_info: ClientInfo) -> Option<TorClient> {
        let mut cmd = tokio::process::Command::new(&client_info.tor_path)
            .args(["-f", &client_info.config_path])
            .stdout(Stdio::piped())
            .spawn()
            .ok()?;

        let stdout = cmd.stdout.as_mut().unwrap();
        let stdout_reader = tokio::io::BufReader::new(stdout);
        let mut stdout_lines = stdout_reader.lines();

        while let Ok(line) = stdout_lines.next_line().await {
            if let Some(line) = line {
                // Check for tor launch success
                if dbg_lvl() >= 4 {
                    println!("[client {}] tor: {}", client_info.client_id, &line);
                }
                
                if line.contains("Bootstrapped 100% (done): Done") {
                    let scheme = format!("socks5://127.0.0.1:{}", client_info.port);
                    if dbg_lvl() >= 1 {
                        println!("[client {}] Initialised client with proxy scheme {}", client_info.client_id, &scheme);
                    }
                    let client = ClientBuilder::new()
                        .connect_timeout(std::time::Duration::from_secs_f64(client_info.timeout))
                        .user_agent("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
                        .proxy(Proxy::all(scheme).unwrap())
                        .build()
                        .unwrap();
                    return Some(TorClient {
                        client_info,
                        client,
                        cmd: Some(cmd),
                    });
                } else if line.contains("[err]") {
                    if dbg_lvl() >= 1 {
                        println!("[client {}] Error creating tor circuit: {}", client_info.client_id, line);
                    }
                    return None;
                }
            }
        }
        None
    }
 
    fn existing_client(client_info: ClientInfo) -> Option<TorClient> {
        let scheme = format!("socks5://127.0.0.1:{}", client_info.port);
        if dbg_lvl() >= 1 {
            println!("[client {}] Using existing client with proxy scheme {}", client_info.client_id, &scheme);
        }
        
        Some(TorClient {
            client_info,
            client: ClientBuilder::new()
                .proxy(Proxy::all(scheme).unwrap())
                .build()
                .ok()?,
            cmd: None,
        })
    }

    pub async fn renew_circuits(&mut self) {
        for tor_client in self.clients.iter_mut() {
            if let Some(new_circ) = Self::load_client(tor_client.client_info.clone()).await {
                *tor_client = new_circ;
            }
        }
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }
    
}

#[derive(Debug)]
pub struct LoadRequest<T: Send> {
    pub req: Request,
    pub data: T,
}

#[derive(Debug)]
pub struct LoadResponse<T: Send> {
    pub response: Response,
    pub req_data: T,
    pub tries: u64,
}

#[derive(Debug)]
pub struct LoadKernel<C> {
    client: Mutex<C>,
    ave_rate: AtomicF64,
    req_count: AtomicUsize,
    reload: AtomicBool,
    ctoken: CancellationToken,
}

pub type InputQueue<T> = Arc<Mutex<Receiver<LoadRequest<T>>>>;
pub type OutputQueue<T> = Receiver<LoadResponse<T>>;
pub type Dump<T> = Arc<Mutex<Vec<LoadRequest<T>>>>;

#[derive(Debug)]
pub struct AsyncLoadManager<C, T: Send> {
    kernels: Vec<Arc<LoadKernel<C>>>,
    input_sender: Sender<LoadRequest<T>>,
    input_queue: InputQueue<T>,
    output_sender: Sender<LoadResponse<T>>,
    output_queue: OutputQueue<T>,
    handles: Vec<JoinHandle<()>>,
    dump: Dump<T>,
}

impl<T: Send + Sync + 'static> AsyncLoadManager<TorClient, T> {
    pub fn execute_tor_kernels(
        &mut self,
        settings: TorKernelSettings,
        tor_mgr: TorClientManager,
    ) {
        self.execute_kernels(settings, tor_mgr.clients.into_iter(), Self::tor_kernel)
    }
}

impl<C, T: Send> AsyncLoadManager<C, T> {
    pub fn new(input_size: usize) -> Self {
        let (input_sender, input_queue) = channel(input_size);
        let (output_sender, output_queue) = channel(input_size);
        Self {
            kernels: Vec::new(),
            input_sender,
            input_queue: Arc::new(Mutex::new(input_queue)),
            output_sender,
            output_queue,
            handles: Vec::new(),
            dump: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn input_sender(&self) -> &Sender<LoadRequest<T>> {
        &self.input_sender
    }

    /// Gets all available output from the output queue.
    pub fn try_responses(&mut self) -> Vec<LoadResponse<T>> {
        let mut out = Vec::new();
        while let Some(next) = self.output_queue.try_recv().ok() {
            out.push(next);
        }
        out
    }

    pub async fn next_response(&mut self) -> Option<LoadResponse<T>> {
        self.output_queue.recv().await
    }

    /// This function is not blocking - blocking can occur if desired by awaiting the returned join handles.
    pub fn execute_kernels<S, F, R>(
        &mut self,
        settings: S,
        clients: impl Iterator<Item = C>,
        mut f: F,
    )
    where
        F: FnMut(usize, S, Arc<LoadKernel<C>>, InputQueue<T>, Sender<LoadResponse<T>>, Dump<T>) -> R,
        R: Future<Output = ()> + Send + 'static,
        F: Copy + Send + 'static,
        S: Clone + Send + 'static,
        T: Send + 'static,
        C: Send + 'static,
    {
        let (mut kernels, mut handles): (Vec<_>, Vec<_>) = clients
            .enumerate()
            .map(|(i, client)| {
                let kernel = Arc::new(LoadKernel {
                    client: Mutex::new(client),
                    ave_rate: AtomicF64::new(f64::NAN),
                    req_count: AtomicUsize::new(0),
                    reload: AtomicBool::new(false),
                    ctoken: CancellationToken::new(),
                });
                (
                    kernel.clone(),
                    tokio::task::spawn(f(
                        i,
                        settings.clone(),
                        kernel,
                        self.input_queue.clone(),
                        self.output_sender.clone(),
                        self.dump.clone(),
                    )),
                )
            })
            .unzip();

        self.kernels.append(&mut kernels);

        self.handles.append(&mut handles);
    }

    pub fn ave_rate(&self) -> f64 {
        let mut c = 0;
        self.kernels
            .iter()
            .filter_map(|k| { 
                let r = k.ave_rate.load(Ordering::Relaxed);
                if r.is_nan() {
                    None
                } else {
                    c += 1;
                    Some(r)
                }
            })
            .fold(0.0, |a, b| a + b)
            / c as f64
    }

    /// Any kernel with an average time above the threshold will have the reload flag set.
    pub fn reload_underperforming_kernels(&self, threshold: f64, min_req: usize) {
        for k in self.kernels.iter() {
            let t = k.ave_rate.load(Ordering::Relaxed);
            let r = k.req_count.load(Ordering::Relaxed);
            if r > min_req && t > threshold && !k.reload.load(Ordering::Relaxed) {
                k.reload.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn kernel_count(&self) -> usize {
        self.kernels.len()
    }

    pub fn send_kernel_kill(&self) {
        for kernel in self.kernels.iter() {
            kernel.ctoken.cancel();
        }
    }

    pub async fn kill_kernels(&mut self) {
        self.send_kernel_kill();
        for h in self.handles.drain(..) {
            let _= h.await;
        }
    }

    /// Extracts the items that have not yet been sent to the output channel.
    pub async fn dump(mut self) -> Vec<T> {
        self.kill_kernels().await;
        // We must wait for the kernels to finish before anything else.
        let mut dump = Vec::new();
        while let Some(next) = self.input_queue.lock().await.try_recv().ok() {
            dump.push(next.data);
        }
        dump.extend(self.dump.lock().await.drain(..).map(|x| x.data));

        dump
    }

    // The function responsible for loading the data.
    pub async fn tor_kernel(
        n: usize,
        settings: TorKernelSettings,
        k: Arc<LoadKernel<TorClient>>,
        iq: InputQueue<T>,
        os: Sender<LoadResponse<T>>,
        dump: Dump<T>,
    ) where
        T: 'static,
    {
        let mut client = k.client.lock().await.client.clone(); // Clone client so we dont have to keep locking the mutex.
        let mut active_req: Vec<LoadRequest<T>> = Vec::new();
        let mut handles: Vec<JoinHandle<Option<LoadRequest<T>>>> = Vec::new();

        let mut ts: Option<SystemTime> = None;

        let error_counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        let mut client_reload_failure: i32 = 0;
        let client_n: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        let min_time = std::time::Duration::from_millis(settings.min_interval); // MINIMUM TIME BETWEEN REQUESTS - any faster is pointless due to server ip limitations!!!


        // Assume our client is loaded properly and begin loop.
        while !k.ctoken.is_cancelled() {
            // First check for reload.
            if k.reload.load(Ordering::Relaxed) || (error_counter.load(Ordering::Relaxed) > settings.errors_before_reset as u32 && settings.errors_before_reset >= 0) {
                {

                }
                k.reload.store(true, Ordering::Relaxed);
                // Kill old client - it uses the same socket so we need it out of the way!
                if let Some(cmd) = &mut k.client.lock().await.cmd {
                    let _ = cmd.kill().await;
                }
                // Reload tor circuit, this will take time.
                let client_info = {
                    let client = k.client.lock().await; // Keep scope small so we can unlock mutex for client load.
                    client.client_info.clone()
                };
                if dbg_lvl() >= 3 {
                    println!("[kernel {}] Reloading kernel tor circuit and client...", n);
                }
                if let Some(new_client) = TorClientManager::load_client(client_info).await {
                    k.ave_rate.store(f64::NAN, Ordering::Relaxed);
                    k.req_count.store(0, Ordering::Relaxed);
                    k.reload.store(false, Ordering::Relaxed); // Store twice in case of weird timing with writes from other threads.
                    
                    client = new_client.client.clone(); // Update our active client.
                    {
                        let mut cl = k.client.lock().await;
                        let clmut: &mut TorClient = cl.deref_mut();
                        *clmut = new_client;
                    }
                    let _ = client_n.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| Some(x + 1)).unwrap_or_else(|x| x);
                    if dbg_lvl() >= 2 {
                        println!("[kernel {}] Reloaded kernel client (number {})", n, client_n.load(Ordering::Relaxed));
                    }
                } else {
                    
                    if dbg_lvl() >= 2 {
                        println!("[kernel {}] Client reload failed!", n);
                    }
                    client_reload_failure += 1;

                    if settings.reload_errors_before_kill >= 0 && client_reload_failure >= settings.reload_errors_before_kill {
                        if dbg_lvl() >= 1 {
                            println!("[kernel {}] Killing kernel due to too many failed reloads", n);
                        }
                        break; // Kill the kernel
                    }
                }
                error_counter.store(0, Ordering::SeqCst);
            } else {
                if handles.len() > settings.max_handles {
                    tokio::time::sleep(std::time::Duration::from_millis(settings.await_downtime)).await;
                } else {
                    // Await next item to process:
                    let load_req: Option<LoadRequest<T>> = if active_req.is_empty() {
                        let mut iq = iq.lock().await;
                        iq.try_recv().ok()
                    } else {
                        Some(active_req.remove(0))
                    };
                    if let Some(req) = load_req.as_ref().and_then(|x| x.req.try_clone()) {
                        if let Some(old_ts) = ts {
                            let now = SystemTime::now();
                            let delta = now.duration_since(old_ts).unwrap();
                            
                            if delta < min_time {
                                let st = min_time - delta;
                                tokio::time::sleep(st).await;
                            }
                            ts = Some(now);
                        } else {
                            ts = Some(SystemTime::now());
                        }

                        let client = client.clone();
                        let os = os.clone();
                        let k = k.clone();
                        let error_counter = error_counter.clone();
                        let client_n = client_n.clone();
                        let initial_client_n = client_n.load(Ordering::Relaxed);
                        let jh = tokio::spawn(async move {
                            let mut i: u64 = 0;
                            'i: while i <= settings.tries && !k.ctoken.is_cancelled() {
                                let ts = SystemTime::now();
                                let r = client.execute(req.try_clone().unwrap()).await;
                                let dur = ts.duration_since(ts).map(|x| x.as_secs_f64());
                                match r {
                                    Ok(response) => {
                                        if response.status().is_success() {
                                            // Only check dur on success, or we get warped results from smaller responses.
                                            if let Ok(dur_secs) = dur {
                                                let ave = k.ave_rate.load(Ordering::Relaxed);
                                                let new_req = k.req_count.load(Ordering::Relaxed) + 1;
                                                k.req_count.store(new_req, Ordering::Relaxed);
                                                let new = ave
                                                    + ((dur_secs - ave)
                                                        / (new_req as f64).min(settings.max_div));
                                                k.ave_rate.store(new, Ordering::Relaxed);
                                            }
                                            // Send back:
                                            let _ = os
                                                .send(LoadResponse {
                                                    response,
                                                    req_data: load_req.unwrap().data,
                                                    tries: i + 1,
                                                })
                                                .await;
                                              
                                            return None;
                                        } else if response.status().is_server_error() {
                                            if i >= settings.tries {
                                                if dbg_lvl() >= 3 {
                                                    println!("[kernel {}] Giving up request - too many tries.", n); 
                                                }
                                                break 'i;
                                            } else {
                                                tokio::time::sleep(std::time::Duration::from_millis(settings.retry_downtime)).await;
                                            }
                                            
                                        } else {
                                            // We don't want to try again because it is unlikely it will work (not a server error)
                                            // Often we get 404 bc the data no longer exits - that is ok.
                                            let _ = os
                                                .send(LoadResponse {
                                                    response,
                                                    req_data: load_req.unwrap().data,
                                                    tries: i + 1,
                                                })
                                                .await;
                                            return None;
                                        }
                                    },
                                    Err(e) => { 
                                        let new_n = client_n.load(Ordering::Relaxed);
                                        if initial_client_n == new_n && k.reload.load(Ordering::Relaxed) == false  {
                                            if dbg_lvl() >= 3 {
                                                println!("[kernel {}] Request failed (client_n={}): {}", n, new_n, e); 
                                            }
                                            let _ = error_counter.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |x| Some(x + 1));
                                        } else {

                                        }
                                        // Place back into queue
                                        break 'i; 
                                    },
                                }
                                i += 1;
                            }
                            load_req // Signifies that we still have to process this...
                        });
                        handles.push(jh);
                    }
                }
            }

            // Attempt to join handles:
            let mut keep: Vec<_> = Vec::new();
            for h in handles {
                if h.is_finished() {
                    if let Ok(Some(r)) = h.await {
                        active_req.push(r);
                    }
                } else {
                    keep.push(h);
                }
            }

            handles = keep;
        }

        if dbg_lvl() >= 1 {
            println!("[kernel {}] Shutting down kernel...", n);
        }

        // Kill tor instance.
        if let Some(cmd) = &mut k.client.lock().await.cmd {
            let _ = cmd.kill().await;
        }

        for h in handles {
            if let Ok(Some(req)) = h.await {
                dump.lock().await.push(req);
            }
        }

        // Dump current item
        {
            let mut dump = dump.lock().await;
            for req in active_req {
                dump.push(req);
            }
        }

        if dbg_lvl() >= 1 {
            println!("[kernel {}] Kernel shut down.", n);
        }
        
    }
}

impl<C, T: Send> Drop for AsyncLoadManager<C, T> {
    fn drop(&mut self) {
        self.send_kernel_kill();
    }
}

#[derive(Debug, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TorKernelSettings {
    max_handles: usize,
    min_interval: u64,
    await_downtime: u64,
    retry_downtime: u64,
    tries: u64,
    max_div: f64,
    errors_before_reset: i32,
    reload_errors_before_kill: i32,
}

impl Default for TorKernelSettings {
    fn default() -> Self {
        Self {
            max_handles: 5,
            min_interval: 500, 
            await_downtime: 100,
            retry_downtime: 200,
            tries: 50,
            max_div: 100.0,
            errors_before_reset: 3,
            reload_errors_before_kill: 2,
        }
    }
}
