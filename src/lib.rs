use std::convert::identity;

use lazy_static::__Deref;

pub mod net;
pub mod table;
pub mod twt;

lazy_static::lazy_static! {
    pub static ref DBG_LVL: u32 = std::env::var("TSA_DBG")
    .ok()
    .map(|x| x.parse::<u32>().ok())
    .and_then(identity)
    .unwrap_or(0);
}

#[inline]
pub(crate) fn dbg_lvl() -> u32 {
    *DBG_LVL.deref()
}
