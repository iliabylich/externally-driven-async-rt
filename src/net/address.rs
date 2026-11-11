use core::net::SocketAddr;

use crate::conversions::u32_to_usize;

use rustix::{
    io::Errno,
    net::{AddressFamily, SocketAddrAny, SocketAddrUnix, addr::SocketAddrArg},
};

pub(crate) trait Address: SocketAddrArg + Sized {
    fn family(&self) -> AddressFamily;

    fn from_any(addr: Option<SocketAddrAny>) -> rustix::io::Result<Self>;
}

impl Address for SocketAddr {
    fn family(&self) -> AddressFamily {
        match self {
            SocketAddr::V4(_) => AddressFamily::INET,
            SocketAddr::V6(_) => AddressFamily::INET6,
        }
    }

    fn from_any(addr: Option<SocketAddrAny>) -> rustix::io::Result<Self> {
        addr.ok_or(Errno::AFNOSUPPORT)?.try_into()
    }
}

impl Address for SocketAddrUnix {
    fn family(&self) -> AddressFamily {
        AddressFamily::UNIX
    }

    fn from_any(addr: Option<SocketAddrAny>) -> rustix::io::Result<Self> {
        match addr {
            Some(addr) if !is_unnamed_unix(&addr) => addr.try_into(),
            _ => SocketAddrUnix::new(""),
        }
    }
}

fn is_unnamed_unix(addr: &SocketAddrAny) -> bool {
    const SUN_PATH_OFFSET: usize = 2;
    // SAFETY: `as_ptr` points to the `addr_len()` initialized bytes of the address.
    let bytes = unsafe {
        core::slice::from_raw_parts(addr.as_ptr().cast::<u8>(), u32_to_usize(addr.addr_len()))
    };
    bytes.get(SUN_PATH_OFFSET).is_none_or(|byte| *byte == 0)
}
