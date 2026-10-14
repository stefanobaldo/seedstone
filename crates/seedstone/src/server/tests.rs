use super::{
    Config, DEFAULT_BIND, EvictionMode, MAX_CLIENTS_REACHED, MemoryLimit, PASSWORD_ENV,
    PasswordSource, SHARDS, SyncPolicy, TcpListener, TcpStream, USAGE, configure_accepted,
    executors, line_for, parse_password_file,
};
use super::{DictSeed, Server, is_recovery_failure};
use seedstone_service::log::{Field, STOPPING};

#[test]
fn fsync_is_parsed_and_defaults_to_interval() {
    let cfg = Config::from_args(std::iter::empty()).unwrap();
    assert_eq!(cfg.fsync, SyncPolicy::INTERVAL);
    assert!(!cfg.fsync_given);
    for (name, policy) in [
        ("always", SyncPolicy::ALWAYS),
        ("interval", SyncPolicy::INTERVAL),
        ("never", SyncPolicy::NEVER),
    ] {
        let cfg = Config::from_args(
            ["--data-dir", "/d", "--fsync", name]
                .into_iter()
                .map(String::from),
        )
        .unwrap();
        assert_eq!(cfg.fsync, policy, "{name}");
        assert!(cfg.fsync_given);
    }
    let given_alone = Config::from_args(["--fsync", "never"].into_iter().map(String::from));
    assert!(
        given_alone.is_ok_and(|cfg| cfg.fsync_given && cfg.data_dir.is_none()),
        "accepted without --data-dir: the binary warns, and it does nothing"
    );
    let refused =
        Config::from_args(["--fsync", "everysec"].into_iter().map(String::from)).unwrap_err();
    assert!(
        refused.contains("--fsync: not a setting: everysec"),
        "{refused}"
    );
    assert!(USAGE.contains("[--fsync always|interval|never]"));
}

#[test]
fn data_dir_is_parsed_and_absent_by_default() {
    let cfg = Config::from_args(std::iter::empty()).unwrap();
    assert_eq!(cfg.data_dir, None);
    let cfg = Config::from_args(
        ["--data-dir", "/var/lib/seedstone"]
            .into_iter()
            .map(String::from),
    )
    .unwrap();
    assert_eq!(
        cfg.data_dir.as_deref(),
        Some(std::path::Path::new("/var/lib/seedstone"))
    );
    assert!(Config::from_args(std::iter::once("--data-dir".to_owned())).is_err());
}

#[test]
fn the_usage_names_the_flag() {
    assert!(USAGE.contains("[--data-dir PATH]"));
}

/// Recovery runs before the listener binds, and a log that cannot be
/// read is told apart from an address that cannot be bound: on an
/// address already taken and a data directory that is a plain file,
/// the refusal is the log's.
#[tokio::test]
async fn a_log_that_cannot_be_read_refuses_the_start_before_the_bind() {
    let file =
        std::env::temp_dir().join(format!("seedstone-not-a-directory-{}", std::process::id()));
    std::fs::write(&file, b"").unwrap();
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = Config {
        bind: taken.local_addr().unwrap(),
        data_dir: Some(file.clone()),
        ..Config::default()
    };
    let refused = Server::bind(cfg, DictSeed { k0: 1, k1: 2 }, String::new()).await;
    std::fs::remove_file(&file).unwrap();
    let Err(error) = refused else {
        panic!("a node over an unreadable log started")
    };
    assert!(is_recovery_failure(&error), "refused for the bind: {error}");
}

/// The binary's lines are the service's format with the wall clock in
/// `ts`: a real reading, and the event's fields after the envelope.
#[test]
fn a_binary_line_is_the_service_format_with_a_wall_clock() {
    let line = line_for(&STOPPING, &[Field::Str("SIGTERM")]);
    assert!(line.starts_with(r#"{"ts":"#), "{line}");
    assert!(
        line.ends_with(r#","level":"info","evt":"stopping","signal":"SIGTERM"}"#),
        "{line}"
    );
    let ts: u64 = line["{\"ts\":".len()..line.find(',').unwrap()]
        .parse()
        .unwrap();
    assert!(ts > 1_700_000_000_000, "not a wall-clock reading: {ts}");
}

/// Whatever this host answers, the count has to be one the pool accepts:
/// at least one executor, never more than there are shards to host.
#[test]
fn the_executor_count_is_one_the_pool_can_be_spawned_with() {
    let executors = executors();
    assert!(executors >= 1, "no executor to host a shard");
    assert!(executors <= SHARDS, "more executors than shards");
}

#[test]
fn args_parse_defaults_and_overrides() {
    let cfg = Config::from_args(std::iter::empty()).unwrap();
    assert_eq!(cfg.bind.to_string(), "127.0.0.1:6379");
    assert_eq!(cfg.max_clients, 10_000);

    // `--no-auth`, because an open bind with no password no longer
    // parses: see `an_open_bind_needs_a_password_or_an_explicit_no_auth`.
    let cfg = Config::from_args(
        ["--bind", "0.0.0.0:7000", "--max-clients", "64", "--no-auth"]
            .iter()
            .map(ToString::to_string),
    )
    .unwrap();
    assert_eq!(cfg.bind.to_string(), "0.0.0.0:7000");
    assert_eq!(cfg.max_clients, 64);
    assert_eq!(cfg.limit, MemoryLimit::default());

    let cfg = Config::from_args(
        ["--maxmemory", "64mb", "--maxmemory-policy", "noeviction"]
            .iter()
            .map(ToString::to_string),
    )
    .unwrap();
    assert_eq!(
        cfg.limit,
        MemoryLimit {
            ceiling: Some(64 << 20),
            mode: EvictionMode::NoEviction,
        }
    );

    // The policy may be named before the ceiling it applies to: the rule
    // is checked after the whole line has been read, not as it is read.
    let cfg = Config::from_args(
        ["--maxmemory-policy", "allkeys-lru", "--maxmemory", "1k"]
            .iter()
            .map(ToString::to_string),
    )
    .unwrap();
    assert_eq!(
        cfg.limit,
        MemoryLimit {
            ceiling: Some(1000),
            mode: EvictionMode::AllKeysLru,
        }
    );
}

#[test]
fn args_reject_garbage() {
    for bad in [
        &["--bind"][..],
        &["--bind", "nonsense"],
        &["--max-clients", "-1"],
        &["--max-clients"],
        &["--max-clients", "0"],
        &["--maxmemory", "lots"],
        &["--maxmemory"],
        // A zero ceiling is a server that may store nothing.
        &["--maxmemory", "0"],
        &["--maxmemory-policy", "volatile-lru"],
        // A policy this server does have, but with no ceiling to reach.
        &["--maxmemory-policy", "allkeys-lru"],
        &["--wat"],
        // A value with no flag in front of it: silently ignoring it would
        // mean a mistyped `--bind` starts a server on the default port.
        &["6379"],
    ] {
        let refusal = Config::from_args(bad.iter().map(ToString::to_string))
            .expect_err(&format!("{bad:?} was accepted"));
        assert!(refusal.contains(USAGE), "{bad:?}: no usage text: {refusal}");
    }
}

/// The default is loopback, and that is a security property rather than a
/// preference — now with two halves. An unconfigured node stays off the
/// network; a node whose operator configured an address off it must say
/// what defends it, which is the refusal below.
#[test]
fn the_default_bind_is_not_reachable_from_a_network() {
    assert!(DEFAULT_BIND.ip().is_loopback());
}

fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |name| {
        pairs
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| (*v).to_owned())
    }
}

/// A directory of this test's own, named after the test and the process,
/// so that a suite running in parallel does not share a password file.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("seedstone-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn an_open_bind_needs_a_password_or_an_explicit_no_auth() {
    let open = ["--bind", "0.0.0.0:7000"].iter().map(ToString::to_string);
    let refusal = Config::from_args_and_env(open.clone(), env(&[]))
        .expect_err("open bind, no password, accepted");
    assert!(
        refusal.contains("--requirepass-file") && refusal.contains("--no-auth"),
        "{refusal}"
    );

    let cfg =
        Config::from_args_and_env(open.clone().chain(["--no-auth".to_owned()]), env(&[])).unwrap();
    assert!(cfg.passwords.is_none() && cfg.no_auth);

    let cfg = Config::from_args_and_env(open, env(&[(PASSWORD_ENV, "pw")])).unwrap();
    assert!(cfg.passwords.as_ref().is_some_and(|s| s.matches(b"pw")));
}

/// One line or two, each a password; the file's final newline is not
/// part of the last one; `CRLF` is a newline.
#[test]
fn a_password_file_holds_one_or_two_lines() {
    let one = parse_password_file(b"pw\n".to_vec()).unwrap();
    assert!(one.matches(b"pw") && one.count() == 1);
    let bare = parse_password_file(b"pw".to_vec()).unwrap();
    assert!(bare.matches(b"pw") && bare.count() == 1);
    let two = parse_password_file(b"old\r\nnew\r\n".to_vec()).unwrap();
    assert!(two.matches(b"old") && two.matches(b"new") && two.count() == 2);
    let two_bare = parse_password_file(b"old\nnew".to_vec()).unwrap();
    assert_eq!(two_bare.count(), 2);
    // Spaces inside a line are the password's.
    let spaced = parse_password_file(b"a b\n".to_vec()).unwrap();
    assert!(spaced.matches(b"a b"));
}

/// Everything else is refused, and the refusal says why.
#[test]
fn a_password_file_with_any_other_shape_is_refused() {
    for (bytes, what) in [
        (&b""[..], "empty"),
        (b"\n", "one empty line"),
        (b"a\n\n", "an empty second line"),
        (b"\nb\n", "an empty first line"),
        (b"a\nb\nc\n", "three lines"),
        (b"   \n", "whitespace only"),
        (b"a\n  \n", "a whitespace-only second line"),
    ] {
        let refusal =
            parse_password_file(bytes.to_vec()).expect_err(&format!("{what} was accepted"));
        assert!(
            refusal.contains("one or two passwords"),
            "{what}: {refusal}"
        );
    }
}

/// The command line records where the password came from, because
/// only a file can be re-read later.
#[test]
fn the_config_records_the_password_source() {
    let dir = scratch("pw-source");
    let path = dir.join("password");
    std::fs::write(&path, "pw\n").unwrap();
    let args = ["--requirepass-file", path.to_str().unwrap()]
        .into_iter()
        .map(ToString::to_string);
    let cfg = Config::from_args_and_env(args, env(&[])).unwrap();
    assert!(cfg.passwords.as_ref().unwrap().matches(b"pw"));
    assert_eq!(
        cfg.source,
        PasswordSource::File(path.to_str().unwrap().to_owned())
    );

    let cfg = Config::from_args_and_env(std::iter::empty(), env(&[(PASSWORD_ENV, "pw")])).unwrap();
    assert!(cfg.passwords.as_ref().unwrap().matches(b"pw"));
    assert_eq!(cfg.source, PasswordSource::Env);

    let cfg = Config::from_args(std::iter::empty()).unwrap();
    assert!(cfg.passwords.is_none());
    assert_eq!(cfg.source, PasswordSource::None);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn contradictory_secrets_are_refused() {
    let dir = scratch("pw2");
    let path = dir.join("password");
    std::fs::write(&path, "pw").unwrap();
    // Collected rather than borrowed: an iterator over an array built
    // inside the closure would outlive the array.
    let file_args = || {
        vec![
            "--requirepass-file".to_owned(),
            path.to_str().unwrap().to_owned(),
        ]
        .into_iter()
    };
    assert!(
        Config::from_args_and_env(file_args(), env(&[(PASSWORD_ENV, "other")])).is_err(),
        "file and env together"
    );
    assert!(
        Config::from_args_and_env(file_args().chain(["--no-auth".to_owned()]), env(&[])).is_err(),
        "--no-auth with a password"
    );
    assert!(
        Config::from_args_and_env(
            ["--requirepass-file", "/nonexistent/p"]
                .iter()
                .map(ToString::to_string),
            env(&[])
        )
        .is_err(),
        "unreadable file"
    );
    std::fs::write(&path, "").unwrap();
    assert!(
        Config::from_args_and_env(file_args(), env(&[])).is_err(),
        "an empty password is no password"
    );
    assert!(
        Config::from_args_and_env(std::iter::empty(), env(&[(PASSWORD_ENV, "")])).is_err(),
        "an empty password is no password, from the environment either"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// The refusal text is what a client matches on, so it is pinned here
/// rather than left to whoever edits the constant next.
#[test]
fn the_refusal_is_byte_exact_to_redis() {
    assert_eq!(MAX_CLIENTS_REACHED, "ERR max number of clients reached");
}

/// A pipelined batch larger than the service's read ceiling spans several
/// drains, so the replies leave in more than one write. With Nagle on,
/// every write after the first waits for the peer's acknowledgement, and a
/// peer that is still waiting for the rest of the batch has nothing to
/// send but a delayed one.
///
/// The option is read back off a real accepted socket rather than the call
/// site being asserted by inspection: `set_nodelay` returning `Ok` says
/// the syscall was accepted, not that this end of this connection is the
/// one carrying the option.
#[tokio::test]
async fn accepted_connections_disable_nagle() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (accepted, _peer) = listener.accept().await.unwrap();

    configure_accepted(&accepted);

    assert!(accepted.nodelay().unwrap(), "Nagle is still on");
    drop(client.await.unwrap());
}

/// A thread stuck in the disk does not keep the process from ending.
#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "a test of how long the process takes to end reads the wall clock"
)]
fn leaving_does_not_wait_for_a_thread_stuck_in_the_disk() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn_blocking(|| std::thread::sleep(std::time::Duration::from_secs(5)));
    let started = std::time::Instant::now();
    super::leave(runtime);
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}
