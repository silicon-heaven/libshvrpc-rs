use std::cmp::min;
use glob::Pattern;
use sha1::Sha1;
use sha1::Digest;
use url::Url;

pub use shvproto::util::parse_log_verbosity;

pub fn sha1_hash(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(&result[..]).as_bytes().to_vec()
}
pub fn sha1_password_hash(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    let mut hash = sha1_hash(password);
    let mut nonce_pass= nonce.to_vec();
    nonce_pass.append(&mut hash);
    sha1_hash(&nonce_pass)
}

pub fn join_path(p1: impl AsRef<str>, p2: impl AsRef<str>) -> String {
    let (p1, p2) = (p1.as_ref(), p2.as_ref());
    if p1.is_empty() && p2.is_empty() {
        "".to_string()
    } else if p1.is_empty() {
        p2.trim_matches('/').to_string()
    } else if p2.is_empty() {
        p1.trim_matches('/').to_string()
    } else {
        p1.trim_matches('/').to_string() + "/" + p2.trim_matches('/')
    }
}

#[macro_export]
macro_rules! join_path {
    ( $first:expr $(, $rest:expr )+ ) => {{
        let mut path = $first.to_string();
        $(
            path = $crate::util::join_path(&path, $rest);
        )+
        path
    }};
}

pub fn starts_with_path(shv_path: impl AsRef<str>, with_path: impl AsRef<str>) -> bool {
    let (shv_path, with_path) = (shv_path.as_ref(), with_path.as_ref());
    let with_path_without_trailing_slash = with_path.strip_suffix('/').unwrap_or(with_path);
    if with_path_without_trailing_slash.is_empty() {
        return true
    }
    shv_path.starts_with(with_path_without_trailing_slash)
        && (shv_path.len() == with_path_without_trailing_slash.len() || shv_path[with_path_without_trailing_slash.len() ..].starts_with('/'))
}
/// Returns `shv_path` without `to_strip` prefix.
///
/// Should behave like:
/// 1. split `shv_path` on `/`
/// 2. split `to_strip` on `/`
/// 3. remove prefix, if any
/// 4. join rest with '/'
pub fn strip_prefix_path<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    if let Some(strip) = path.strip_prefix(prefix) {
        if strip.is_empty() {
            Some(strip)
        } else {
            match strip.strip_prefix('/') {
                None => {
                    if prefix.is_empty() {
                        Some(strip)
                    } else {
                        None
                    }
                }
                Some(strip) => { Some(strip) }
            }
        }
    } else {
        None
    }
}

pub fn login_from_url(url: &Url) -> (String, String) {
    let mut user = "".to_string();
    let mut password = "".to_string();
    for (key,val) in url.query_pairs() {
        if key == "user" {
            user = val.to_string();
        } else if key == "password" {
            password = val.to_string();
        }
    }
    if user.is_empty() {
        user = url.username().to_string();
    }
    if password.is_empty() {
        password = url.password().unwrap_or_default().to_string();
    }
    (user, password)
}

pub fn glob_len(glob: &str) -> usize {
    // number of slashes + 1
    glob.split('/').count()
}
pub fn left_glob(glob: &str, glob_len: usize) -> Option<&str> {
    let mut ix: usize = 0;
    let mut n: usize = 0;
    for p in glob.splitn(glob_len + 1, '/') {
        ix += p.len();
        n += 1;
        if n == glob_len {
            break
        }
    }
    if n == glob_len {
        ix += n - 1; // add intermediate slashes
        Some(&glob[0..ix])
    } else {
        None
    }
}
pub fn split_glob_on_match<'a>(glob_pattern: &'a str, shv_path: &str) -> Result<Option<(&'a str, &'a str)>, String> {
    if glob_pattern.is_empty() {
        return Ok(None);
    }
    // find first '**' occurrence in paths
    let globstar_pos = glob_pattern.find("**");
    let pattern1 = match globstar_pos {
        None => { glob_pattern }
        Some(ix) => {
            if ix == 0 { "" } else { &glob_pattern[0 .. (ix - 1)] }
        }
    };
    if globstar_pos.is_some() && pattern1.is_empty() {
        // paths starts with **, this matches everything
        return Ok(Some(("**", glob_pattern)))
    }
    if pattern1.is_empty() { return Err("Valid glob pattern cannot be empty".into()) };
    if shv_path.is_empty() { return Err("Valid mount point cannot be empty".into()) };
    let shv_path_glen = glob_len(shv_path);
    let pattern1_glen = glob_len(pattern1);
    let match_len = min(shv_path_glen, pattern1_glen);
    let trimmed_pattern1 = left_glob(pattern1, match_len).unwrap();
    let trimmed_path = left_glob(shv_path, match_len).unwrap();
    let pattern = Pattern::new(trimmed_pattern1).map_err(|err| err.to_string())?;
    if pattern.matches(trimmed_path) {
        match globstar_pos {
            None => {
                // We don't probably want to use `cmp()` and match, as it might be slower:
                // https://rust-lang.github.io/rust-clippy/master/index.html#/comparison_chain
                #[allow(clippy::comparison_chain)]
                if shv_path_glen > pattern1_glen {
                    // a/b vs a/b/c
                    Ok(None)
                } else if shv_path_glen == pattern1_glen {
                    // a/b/c vs a/b/c
                    Ok(Some((trimmed_pattern1, "")))
                } else {
                    // a/b/c vs a/b
                    Ok(Some((trimmed_pattern1, &glob_pattern[(trimmed_pattern1.len()+1) .. ])))
                }
            }
            Some(ix) => {
                if shv_path_glen > pattern1_glen {
                    // a/b/** vs a/b/c
                    Ok(Some((&glob_pattern[0 .. (ix+2)], &glob_pattern[ix ..])))
                } else {
                    // a/b/c/** vs a/b/c
                    // a/b/c/d/** vs a/b/c
                    Ok(Some((trimmed_pattern1, &glob_pattern[trimmed_pattern1.len()+1 ..])))
                }
            }
        }
    } else {
        Ok(None)
    }
}
pub fn hex_string(data: &[u8], delim: Option<&str>) -> String {
    let mut ret = "".to_string();
    for b in data {
        if let Some(delim) = delim {
            if ret.len() > 1 {
                ret += delim;
            }
        }
        ret += &format!("{:02x}", b);
    }
    ret
}

#[cfg(test)]
mod tests {
    use crate::util::{glob_len, left_glob, split_glob_on_match, starts_with_path, strip_prefix_path};
    fn init_log() {
        let _ = env_logger::builder()
            // .filter(None, LevelFilter::Debug)
            .is_test(true).try_init();
    }

    #[test]
    fn test_glob_len() {
        let data = vec![
            ("", 1usize),
            ("/", 2usize),
            ("a", 1usize),
            ("a/b/c", 3usize),
            ("a/b/", 3usize),
        ];
        for (g, n) in data {
            assert_eq!(glob_len(g), n);
        }
    }
    #[test]
    fn test_left_glob() {
        let data = vec![
            ("", 1usize, Some("")),
            ("a", 1usize, Some("a")),
            ("a", 2usize, None),
            ("a/b", 1usize, Some("a")),
            ("a/b", 2usize, Some("a/b")),
            ("a/b", 3usize, None),
        ];
        for (glob, len, trimmed) in data {
            assert_eq!(left_glob(glob, len), trimmed);
        }
    }
    #[test]
    fn test_split_glob_on_match() {
        let data = vec![
            ("", "a/b/c", None),
            ("a", "a/b/c", None),
            ("a/b", "a/b/c", None),
            ("a/b/c", "a/b/c", Some(("a/b/c", ""))),
            ("a/b/c/d", "a/b/c", Some(("a/b/c", "d"))),
            ("a/b/c", "a", Some(("a", "b/c"))),
            ("a/b/c", "a/b", Some(("a/b", "c"))),
            ("a/b/c", "a/b/c/d", None),
            ("a/b/c", "a/b/d", None),
            ("**", "a/b/c", Some(("**", "**"))),
            ("a/**", "a/b/c", Some(("a/**", "**"))),
            ("a/**/c", "a/b/c", Some(("a/**", "**/c"))),
            ("a/b/c/**", "a/b/c", Some(("a/b/c", "**"))),
            ("a/b*/c/**", "a/b/c", Some(("a/b*/c", "**"))),
            ("?/b*/c/**", "a/b/c", Some(("?/b*/c", "**"))),
            ("a/b/c/**/d/e/**", "a/b/c", Some(("a/b/c", "**/d/e/**"))),
            ("**/a/b", "a/b/c", Some(("**", "**/a/b"))),
        ];
        for (glob, path, result) in data {
            assert_eq!(split_glob_on_match(glob, path), Ok(result));
        }
    }
    #[test]
    fn test_start_with_path() {
        let data = vec![
            ("", "", true),
            ("a", "", true),
            ("", "a", false),
            ("a/b/c", "a/b/c", true),
            ("a/b/c", "a/b/", true),
            ("a/b/c", "a/b", true),
            ("a/b/c", "b/b", false),
        ];
        for (path, with_path, res) in data {
            //println!("path: {path}, with: {with_path}");
            assert_eq!(starts_with_path(path, with_path), res);
        }
    }
    #[test]
    fn test_strip_path() {
        init_log();
        let data = vec![
            ("", "", Some("")),
            ("", "/", Some("")),
            ("", "/a", Some("a")),
            ("", "a", Some("a")),
            ("/", "", None),
            ("a", "", None),
            ("a/", "a", None),
            ("a/b/c", "a/b/c", Some("")),
            ("a/b/", "a/b/c", None),
            ("a/b", "a/b/c", Some("c")),
            ("b/b", "a/b/c", None),
            ("a", "abc", None),
            ("a/b", "a/bc", None),
        ];
        for (prefix, path, res) in data {
            //debug!("prefix: {prefix}, path: {path}");
            assert_eq!(strip_prefix_path(path, prefix), res);
        }
    }

    #[test]
    fn join_multiple_path_segments() {
        let foo = "foo".to_string();
        let bar = "bar".to_string();
        let baz = "baz".to_string();
        assert_eq!(join_path!("foo", "bar", "baz"), "foo/bar/baz".to_string());
        assert_eq!(join_path!(foo, "bar", "baz"), "foo/bar/baz".to_string());
        assert_eq!(join_path!("foo", bar.clone(), "baz"), "foo/bar/baz".to_string());
        assert_eq!(join_path!(foo, bar, baz), "foo/bar/baz".to_string());
    }

}
