//! The peers setting's names, kept resolved. Machines change address as
//! routine, so a name is looked up again for each announcement, and on a
//! thread of its own: a lookup can block for seconds, and the node must not
//! block with it.

use std::collections::BTreeSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The addresses `named` have now, and the record's lines for those that
/// fail: each name once, until it resolves again. `failing` holds the names
/// the record has named.
fn resolve(
    named: &[String],
    failing: &mut BTreeSet<String>,
    lookup: impl Fn(&str) -> Option<SocketAddr>,
) -> (Vec<SocketAddr>, Vec<String>) {
    let (mut addresses, mut lines) = (Vec::new(), Vec::new());
    for name in named {
        if let Some(address) = lookup(name) {
            failing.remove(name);
            addresses.push(address);
        } else if failing.insert(name.clone()) {
            lines.push(format!(
                "smugglr mesh: the peer {name} does not resolve; trying again at each announcement"
            ));
        }
    }
    (addresses, lines)
}

/// The address `name` has now over the system's resolver, which covers mDNS.
fn lookup(name: &str) -> Option<SocketAddr> {
    name.to_socket_addrs().ok()?.find(SocketAddr::is_ipv4)
}

/// The addresses among `named` that need no lookup, so a node can announce
/// to them from its first moment.
pub(crate) fn literal(named: &[String]) -> Vec<SocketAddr> {
    named.iter().filter_map(|name| name.parse().ok()).collect()
}

/// Keeps `found` at what `named` resolve to, every `every`, until `running`
/// turns false.
pub(crate) fn keep_resolved(
    named: &[String],
    found: &Mutex<Vec<SocketAddr>>,
    running: &AtomicBool,
    every: Duration,
) {
    let mut failing = BTreeSet::new();
    while running.load(Ordering::Relaxed) {
        let (addresses, lines) = resolve(named, &mut failing, lookup);
        for line in lines {
            println!("{line}");
        }
        if let Ok(mut found) = found.lock() {
            *found = addresses;
        }
        let looked = Instant::now();
        while running.load(Ordering::Relaxed) && looked.elapsed() < every {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::net::SocketAddr;

    use super::{literal, resolve};

    fn address(last: u8) -> SocketAddr {
        SocketAddr::from(([192, 168, 88, last], 31_415))
    }

    #[test]
    fn a_name_whose_address_changes_gives_the_new_one_at_the_next_look() {
        let named = ["rzr.local:31415".to_owned()];
        let mut failing = BTreeSet::new();
        let now = RefCell::new(address(8));
        let look = |_: &str| Some(*now.borrow());
        assert_eq!(resolve(&named, &mut failing, look).0, [address(8)]);
        *now.borrow_mut() = address(44);
        assert_eq!(resolve(&named, &mut failing, look).0, [address(44)]);
    }

    #[test]
    fn a_name_that_does_not_resolve_costs_only_itself_and_is_named_once() {
        let named = ["gone.local:31415".to_owned(), "rzr.local:31415".to_owned()];
        let mut failing = BTreeSet::new();
        let look = |name: &str| name.starts_with("rzr").then(|| address(8));
        let (addresses, lines) = resolve(&named, &mut failing, look);
        assert_eq!(addresses, [address(8)]);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("gone.local"));
        assert_eq!(resolve(&named, &mut failing, look).1.len(), 0);
    }

    #[test]
    fn a_name_that_resolves_again_is_named_again_when_it_next_fails() {
        let named = ["rzr.local:31415".to_owned()];
        let mut failing = BTreeSet::new();
        assert_eq!(resolve(&named, &mut failing, |_| None).1.len(), 1);
        assert_eq!(
            resolve(&named, &mut failing, |_| Some(address(8))).1.len(),
            0
        );
        assert_eq!(resolve(&named, &mut failing, |_| None).1.len(), 1);
    }

    #[test]
    fn an_address_needs_no_lookup_and_a_name_does() {
        let named = ["10.0.0.2:31415".to_owned(), "rzr.local:31415".to_owned()];
        assert_eq!(literal(&named), [SocketAddr::from(([10, 0, 0, 2], 31_415))]);
    }
}
