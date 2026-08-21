//! Glob matching, over bytes.
//!
//! The semantics are Redis's: `*` for any run, `?` for exactly one byte, a
//! bracketed class with ranges and negation, and `\` to take the next byte
//! literally. Keys are byte strings, so this never decodes text — a pattern
//! and a key are both sequences of bytes and a class member is a byte.
//!
//! The reference is `stringmatchlen` in Redis's `util.c`, followed byte for
//! byte with one deliberate departure: a single `*` against an empty subject
//! matches here and does not there. Redis can afford to answer no because
//! `KEYS` short-circuits the one-byte `*` pattern through its all-keys path
//! without ever calling the matcher — so a key named `""` is still returned
//! by `KEYS *`. Having no such short-circuit, this matcher has to answer yes
//! to a star against an empty subject for `KEYS *` to behave as Redis's does,
//! and the residue of that is the multi-star case: a run of two or more stars
//! matches an empty subject here too.
//!
//! The implementation is iterative with one remembered star, not recursive.
//! A recursive matcher on a pattern of many stars retries exponentially, and
//! this runs once per key of a keyspace walk on the request path.

/// Whether `subject` matches `pattern`.
#[must_use]
pub fn matches(pattern: &[u8], subject: &[u8]) -> bool {
    let (mut p, mut s) = (0usize, 0usize);
    // Where to resume if the current attempt fails: the star's position in the
    // pattern, and the next subject byte it should swallow.
    let (mut star, mut resume) = (None, 0usize);

    while s < subject.len() {
        match pattern.get(p) {
            Some(b'*') => {
                star = Some(p);
                resume = s;
                p += 1;
            }
            Some(b'?') => {
                p += 1;
                s += 1;
            }
            Some(b'[') => {
                let (hit, width) = class(&pattern[p..], subject[s]);
                if hit {
                    p += width;
                    s += 1;
                } else if !backtrack(&mut p, &mut s, star, &mut resume) {
                    return false;
                }
            }
            Some(b'\\') if p + 1 < pattern.len() => {
                if pattern[p + 1] == subject[s] {
                    p += 2;
                    s += 1;
                } else if !backtrack(&mut p, &mut s, star, &mut resume) {
                    return false;
                }
            }
            Some(&literal) if literal == subject[s] => {
                p += 1;
                s += 1;
            }
            _ => {
                if !backtrack(&mut p, &mut s, star, &mut resume) {
                    return false;
                }
            }
        }
    }

    // The subject is spent; the pattern matches only if what is left is stars.
    pattern[p.min(pattern.len())..].iter().all(|&b| b == b'*')
}

/// Gives the current attempt back to the last star and advances it one byte.
///
/// Returns false when there is no star to give it back to, which is a
/// definitive mismatch.
const fn backtrack(p: &mut usize, s: &mut usize, star: Option<usize>, resume: &mut usize) -> bool {
    match star {
        Some(at) => {
            *p = at + 1;
            *resume += 1;
            *s = *resume;
            true
        }
        None => false,
    }
}

/// Matches one byte against a class at the head of `pattern`.
///
/// Answers `(hit, width)`: whether `byte` is a member, and the class's width
/// in bytes, counting the opening bracket and — when there is one — the
/// closing one.
///
/// This answers a width rather than reporting an unterminated class back to
/// the caller because there is no shape under which the bracket becomes a
/// literal: a class that is never closed runs to the end of the pattern with
/// the members it collected and still takes a byte, so `[a` matches `a` and
/// not `[a`.
fn class(pattern: &[u8], byte: u8) -> (bool, usize) {
    let mut i = 1;
    let negated = pattern.get(i) == Some(&b'^');
    if negated {
        i += 1;
    }
    let mut hit = false;
    loop {
        match pattern.get(i) {
            // The pattern ran out: the class ends here, unclosed.
            None => return (hit != negated, i),
            // The first `]` closes it, wherever it stands. There is no
            // exception for one in first position: `[]a]` is an empty class —
            // which matches nothing — followed by the literals `a]`.
            Some(b']') => return (hit != negated, i + 1),
            Some(b'\\') if i + 1 < pattern.len() => {
                hit |= pattern[i + 1] == byte;
                i += 2;
            }
            Some(&member) => {
                // A range is any member, `-`, and one more byte — `]`
                // included, which is why `[a-]` is the range `]..=a` with its
                // ends swapped rather than three literals.
                if pattern.get(i + 1) == Some(&b'-')
                    && let Some(&end) = pattern.get(i + 2)
                {
                    let (lo, hi) = (member.min(end), member.max(end));
                    hit |= (lo..=hi).contains(&byte);
                    i += 3;
                } else {
                    hit |= member == byte;
                    i += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pattern_without_metacharacters_is_an_equality_test() {
        assert!(matches(b"abc", b"abc"));
        assert!(!matches(b"abc", b"abd"));
        assert!(!matches(b"abc", b"abcd"));
        assert!(!matches(b"abcd", b"abc"));
    }

    #[test]
    fn star_takes_any_run_including_none() {
        assert!(matches(b"*", b""));
        assert!(matches(b"*", b"anything"));
        assert!(matches(b"a*c", b"ac"));
        assert!(matches(b"a*c", b"abbbc"));
        assert!(!matches(b"a*c", b"abbb"));
        // Backtracking: the first star must give ground for the second half
        // to match, which a greedy scan without retry gets wrong.
        assert!(matches(b"*abc", b"zzabcabc"));
        assert!(matches(b"a*b*c", b"axxbyyc"));
    }

    #[test]
    fn question_takes_exactly_one_byte() {
        assert!(matches(b"a?c", b"abc"));
        assert!(!matches(b"a?c", b"ac"));
        assert!(!matches(b"a?c", b"abbc"));
    }

    #[test]
    fn a_class_matches_one_of_its_members() {
        assert!(matches(b"[abc]", b"b"));
        assert!(!matches(b"[abc]", b"d"));
        assert!(matches(b"[a-c]", b"b"));
        assert!(!matches(b"[a-c]", b"d"));
        assert!(matches(b"[^a]", b"b"));
        assert!(!matches(b"[^a]", b"a"));
        // A class that is never closed runs to the end of the pattern with the
        // members it collected and still takes one byte, so the bracket never
        // becomes a literal.
        assert!(matches(b"[a", b"a"));
        assert!(!matches(b"[abc", b"[abc"));
        assert!(matches(b"[^]", b"x"));
        // `]` closes a class wherever it stands, including in first position:
        // `[]a]` is an empty class, which matches nothing, and then `a]`.
        assert!(!matches(b"[]a]", b"a"));
        assert!(!matches(b"[^]]", b"a"));
        // A range may end at `]`.
        assert!(matches(b"[a-]", b"_"));
        assert!(!matches(b"[a-]", b"-"));
    }

    #[test]
    fn a_backslash_makes_the_next_byte_literal() {
        assert!(matches(br"a\*c", b"a*c"));
        assert!(!matches(br"a\*c", b"abc"));
        assert!(matches(br"\[a]", b"[a]"));
        // A trailing backslash matches a trailing backslash.
        assert!(matches(br"a\", br"a\"));
    }

    #[test]
    fn keys_and_patterns_are_bytes_not_text() {
        assert!(matches(b"a*", &[b'a', 0x00, 0xff]));
        assert!(matches(&[0xff], &[0xff]));
    }

    #[test]
    fn a_run_of_stars_does_not_blow_up() {
        // Pathological backtracking is the classic failure of a naive
        // recursive matcher. This must return, and quickly.
        let pattern = b"*a*a*a*a*a*a*a*a*b";
        let subject = vec![b'a'; 64];
        assert!(!matches(pattern, &subject));
    }
}
