//! Self-only getrusage telemetry. Peak RSS is since process start, not current RSS.
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Usage {
    pub cpu_seconds: f64,
    pub peak_rss_bytes: u64,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub fn usage() -> Option<Usage> {
    use std::os::raw::{c_int, c_long};
    #[derive(Default)]
    #[repr(C)]
    struct Timeval {
        seconds: c_long,
        #[cfg(target_os = "macos")]
        micros: i32,
        #[cfg(target_os = "linux")]
        micros: c_long,
    }
    #[derive(Default)]
    #[repr(C)]
    struct Rusage {
        user: Timeval,
        system: Timeval,
        maxrss: c_long,
        unused: [c_long; 13],
    }
    unsafe extern "C" {
        fn getrusage(who: c_int, usage: *mut Rusage) -> c_int;
    }
    let mut raw = Rusage::default();
    // SAFETY: Rusage matches sys/resource.h for the two cfg-gated platforms;
    // the pointer is valid, writable, correctly aligned, and retained for this call.
    if unsafe { getrusage(0, &mut raw) } != 0 {
        return None;
    }
    let cpu_seconds = (raw.user.seconds + raw.system.seconds) as f64
        + (raw.user.micros + raw.system.micros) as f64 / 1e6;
    #[cfg(target_os = "macos")]
    let peak_rss_bytes = raw.maxrss as u64;
    #[cfg(target_os = "linux")]
    let peak_rss_bytes = raw.maxrss as u64 * 1024;
    Some(Usage {
        cpu_seconds,
        peak_rss_bytes,
    })
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn usage() -> Option<Usage> {
    None
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn self_resource_counters_are_available_and_monotone() {
        let before = super::usage().unwrap();
        let mut bytes = vec![0_u8; 1024 * 1024];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = std::hint::black_box(i as u8);
        }
        std::hint::black_box(bytes);
        let after = super::usage().unwrap();
        assert!(after.cpu_seconds >= before.cpu_seconds);
        assert!(after.peak_rss_bytes >= before.peak_rss_bytes && after.peak_rss_bytes > 0);
    }
}
