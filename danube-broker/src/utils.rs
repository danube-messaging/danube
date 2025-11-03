use rand::{rng, Rng};

pub(crate) fn get_random_id() -> u64 {
    rng().random::<u64>()
}

pub(crate) fn join_path(parts: &[&str]) -> String {
    let mut result = String::new();

    for (i, part) in parts.iter().enumerate() {
        // Remove trailing slashes from all parts
        let part = part.trim_end_matches('/');

        if i == 0 {
            result.push_str(part.trim_end_matches('/'));
        } else {
            if !part.is_empty() && !part.starts_with('/') {
                result.push('/');
            }
            result.push_str(part);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let parts = ["/one/two/three", "four", "five"];
        assert_eq!(join_path(&parts), "/one/two/three/four/five");
    }

    #[test]
    fn test_all_parts_with_slash() {
        let parts = ["/one", "/two", "/three"];
        assert_eq!(join_path(&parts), "/one/two/three");
    }

    #[test]
    fn test_parts_with_trailing_slashes() {
        let parts = ["/with/", "trailing/", "/slashes/"];
        assert_eq!(join_path(&parts), "/with/trailing/slashes");
    }
}
