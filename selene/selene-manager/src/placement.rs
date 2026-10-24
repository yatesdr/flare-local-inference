//! Rendezvous placement for upstream selection.
//!
//! `score(node) = xxh3_64(node_name || namespace_id || root_key) * weight`, computed in `u128`.
//! Highest score wins; equal scores break by ascending node name (byte-wise).

use std::hash::Hasher;
use twox_hash::XxHash3_64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Node<'a> {
    pub name: &'a str,
    pub weight: u64,
}

/// The bytes hashed for a chain: `consumed_tokens` little-endian, then the 16-byte digest.
pub fn root_key_bytes(consumed_tokens: u64, digest: &[u8; 16]) -> [u8; 24] {
    let mut bytes = [0u8; 24];
    bytes[..8].copy_from_slice(&consumed_tokens.to_le_bytes());
    bytes[8..].copy_from_slice(digest);
    bytes
}

/// xxh3-64 of `node_name || namespace_id || root_key`, multiplied by `weight` in `u128`.
pub fn score(node: &str, namespace: &[u8; 32], root_key: &[u8; 24], weight: u64) -> u128 {
    let mut hasher = XxHash3_64::new();
    hasher.write(node.as_bytes());
    hasher.write(namespace);
    hasher.write(root_key);
    u128::from(hasher.finish()) * u128::from(weight)
}

/// True when `(score, name)` beats `(best, best_name)`.
pub fn better(score: u128, name: &str, best: u128, best_name: &str) -> bool {
    score > best || (score == best && name.as_bytes() < best_name.as_bytes())
}

/// The node that owns a chain, or `None` when no nodes are configured.
pub fn choose<'a>(
    namespace: &[u8; 32],
    root_key: &[u8; 24],
    nodes: &[Node<'a>],
) -> Option<&'a str> {
    let mut winner: Option<(u128, &'a str)> = None;
    for node in nodes {
        let node_score = score(node.name, namespace, root_key, node.weight);
        let replace = match winner {
            None => true,
            Some((best, best_name)) => better(node_score, node.name, best, best_name),
        };
        if replace {
            winner = Some((node_score, node.name));
        }
    }
    winner.map(|(_, name)| name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = index as u8;
        }
        bytes
    }

    fn nodes(weights: [u64; 3]) -> [Node<'static>; 3] {
        [
            Node {
                name: "alpha",
                weight: weights[0],
            },
            Node {
                name: "beta",
                weight: weights[1],
            },
            Node {
                name: "gamma",
                weight: weights[2],
            },
        ]
    }

    #[test]
    fn root_key_is_consumed_tokens_le_then_digest() {
        let bytes = root_key_bytes(1024, &[0xaa; 16]);
        assert_eq!(&bytes[..8], &[0x00, 0x04, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&bytes[8..], &[0xaa; 16]);
    }

    #[test]
    fn vector_a_selects_beta() {
        let ns = namespace();
        let root = root_key_bytes(1024, &[0xaa; 16]);
        assert_eq!(score("alpha", &ns, &root, 1), 0x5e25_289e_bb59_d9be);
        assert_eq!(score("beta", &ns, &root, 1), 0xb9fe_aca3_29f6_2d4a);
        assert_eq!(score("gamma", &ns, &root, 1), 0x170c_ad79_c9ff_10ef);
        assert_eq!(choose(&ns, &root, &nodes([1, 1, 1])), Some("beta"));
    }

    #[test]
    fn vector_b_selects_alpha() {
        let ns = namespace();
        let root = root_key_bytes(256, &[0xbb; 16]);
        assert_eq!(score("alpha", &ns, &root, 1), 0xdc70_bd3e_12fb_4ce2);
        assert_eq!(score("beta", &ns, &root, 1), 0x3392_a664_c31e_e079);
        assert_eq!(score("gamma", &ns, &root, 1), 0x0f68_c6ec_e603_af22);
        assert_eq!(choose(&ns, &root, &nodes([1, 1, 1])), Some("alpha"));
    }

    #[test]
    fn vector_c_weight_selects_gamma() {
        let ns = namespace();
        let root = root_key_bytes(1024, &[0xaa; 16]);
        assert_eq!(
            score("gamma", &ns, &root, 9),
            u128::from(0x170c_ad79_c9ff_10efu64) * 9
        );
        assert_eq!(choose(&ns, &root, &nodes([1, 1, 9])), Some("gamma"));
    }

    #[test]
    fn equal_scores_break_by_name() {
        assert!(better(5, "a", 5, "b"));
        assert!(!better(5, "c", 5, "b"));
        assert!(better(6, "z", 5, "a"));
        assert!(!better(4, "a", 5, "z"));
    }

    #[test]
    fn weighting_changes_the_owner_without_changing_the_hash() {
        let ns = namespace();
        let root = root_key_bytes(1024, &[0xaa; 16]);
        let plain = score("gamma", &ns, &root, 1);
        assert_eq!(plain, 0x170c_ad79_c9ff_10ef);
        assert_eq!(score("gamma", &ns, &root, 3), plain * 3);
    }

    #[test]
    fn adding_a_node_only_moves_keys_to_the_new_node() {
        let ns = namespace();
        let three = nodes([1, 1, 1]);
        let four = [
            three[0],
            three[1],
            three[2],
            Node {
                name: "delta",
                weight: 1,
            },
        ];
        for consumed in 0..256u64 {
            let root = root_key_bytes(consumed * 64, &[consumed as u8; 16]);
            let before = choose(&ns, &root, &three).unwrap();
            let after = choose(&ns, &root, &four).unwrap();
            assert!(before == after || after == "delta", "{before} -> {after}");
        }
    }

    #[test]
    fn no_nodes_is_no_owner() {
        assert_eq!(
            choose(&namespace(), &root_key_bytes(1, &[0; 16]), &[]),
            None
        );
    }
}
