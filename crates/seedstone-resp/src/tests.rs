use super::*;

#[test]
fn encodes_every_frame_type() {
    let cases: &[(Frame, &[u8])] = &[
        (Frame::Simple("OK".into()), b"+OK\r\n"),
        (Frame::Error("ERR boom".into()), b"-ERR boom\r\n"),
        (Frame::Integer(-7), b":-7\r\n"),
        (Frame::Bulk(b"hi".to_vec()), b"$2\r\nhi\r\n"),
        // The empty bulk is the one length-prefixed frame whose payload
        // and terminator are adjacent, so it is exactly where an
        // off-by-one in the encoder would hide. It is also not `Null`:
        // "a value that is zero bytes long" and "no value" are different
        // frames, and the pair is here so nobody collapses them.
        (Frame::Bulk(Vec::new()), b"$0\r\n\r\n"),
        (Frame::Null, b"$-1\r\n"),
        (
            Frame::Array(vec![
                Frame::Bulk(b"GET".to_vec()),
                Frame::Bulk(b"k".to_vec()),
            ]),
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
        ),
    ];
    for (frame, wire) in cases {
        let mut out = Vec::new();
        encode(frame, &mut out);
        assert_eq!(&out, wire);
    }
}

#[test]
fn parse_round_trips_every_frame_type() {
    let cases: &[Frame] = &[
        Frame::Simple("OK".into()),
        Frame::Error("ERR boom".into()),
        Frame::Integer(-7),
        Frame::Bulk(b"hi".to_vec()),
        Frame::Null,
        Frame::Array(vec![
            Frame::Bulk(b"GET".to_vec()),
            Frame::Bulk(b"k".to_vec()),
        ]),
        Frame::Array(vec![]),
        Frame::Array(vec![Frame::Array(vec![Frame::Integer(1)])]),
    ];
    for frame in cases {
        let mut out = Vec::new();
        encode(frame, &mut out);
        let (parsed, consumed) = parse(&out).unwrap().unwrap();
        assert_eq!(&parsed, frame);
        assert_eq!(consumed, out.len());
    }
}

#[test]
fn bulk_strings_carry_arbitrary_bytes() {
    // The length prefix is what makes a bulk string binary-safe: a payload
    // holding the terminator itself, a NUL and a non-UTF-8 byte must come
    // back byte for byte.
    let frame = Frame::Bulk(b"a\r\nb\x00c\xffd".to_vec());
    let mut out = Vec::new();
    encode(&frame, &mut out);
    let (parsed, consumed) = parse(&out).unwrap().unwrap();
    assert_eq!(parsed, frame);
    assert_eq!(consumed, out.len());
}

#[test]
fn integers_round_trip_at_the_extremes_of_i64() {
    for value in [i64::MIN, i64::MAX] {
        let frame = Frame::Integer(value);
        let mut out = Vec::new();
        encode(&frame, &mut out);
        let (parsed, consumed) = parse(&out).unwrap().unwrap();
        assert_eq!(parsed, frame, "{value}");
        assert_eq!(consumed, out.len(), "{value}");
    }
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "must not contain CR or LF")]
fn encoding_a_simple_string_holding_a_terminator_trips_the_debug_assertion() {
    let mut out = Vec::new();
    encode(&Frame::Simple("OK\r\n+INJECTED".into()), &mut out);
}

#[test]
fn parse_returns_none_on_partial_input() {
    let mut bulk = Vec::new();
    encode(&Frame::Bulk(b"hello".to_vec()), &mut bulk);

    let mut nested_array = Vec::new();
    encode(
        &Frame::Array(vec![Frame::Array(vec![Frame::Bulk(b"x".to_vec())])]),
        &mut nested_array,
    );

    for out in [&bulk, &nested_array] {
        for cut in 0..out.len() {
            assert_eq!(parse(&out[..cut]).unwrap(), None, "cut at {cut} of {out:?}");
        }
    }
}

#[test]
fn parse_returns_none_on_specific_incomplete_inputs() {
    for buf in [
        &b"$2\r\nhi\r"[..], // a lone trailing `\r`, not yet `\r\n`
        b"*3\r\n",          // an array header with no elements after it
    ] {
        assert_eq!(parse(buf).unwrap(), None, "{buf:?}");
    }
}

#[test]
fn parse_rejects_malformed_input() {
    for bad in [
        &b"$abc\r\n"[..],
        b"!5\r\n",
        b"*1\r\n:x\r\n",
        b"$3\r\nabcd\r\n",
        b"*-1\r\n",                   // negative array length, including the null array
        b"$99999999999999999999\r\n", // bulk length out of i64 range
        b"$2\r\nhi\n\r",              // terminator bytes in the wrong order
    ] {
        assert!(parse(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn parse_enforces_the_array_depth_limit() {
    // 64 levels of array nesting are accepted...
    let mut frame = Frame::Integer(1);
    for _ in 0..64 {
        frame = Frame::Array(vec![frame]);
    }
    let mut buf = Vec::new();
    encode(&frame, &mut buf);
    let (parsed, consumed) = parse(&buf).unwrap().unwrap();
    assert_eq!(parsed, frame);
    assert_eq!(consumed, buf.len());

    // ...but the 65th level is rejected.
    let too_deep = Frame::Array(vec![frame]);
    let mut buf = Vec::new();
    encode(&too_deep, &mut buf);
    assert!(parse(&buf).is_err());
}

#[test]
fn parse_enforces_the_length_ceilings_at_the_header() {
    // Rejected the moment the length line is read — no payload byte is
    // ever buffered, which is the whole point of the ceiling.
    let over = MAX_BULK_LEN + 1;
    assert!(parse(format!("${over}\r\n").as_bytes()).is_err());
    // The boundary itself is still accepted, and still reports "need
    // more bytes" rather than an error.
    assert_eq!(parse(format!("${MAX_BULK_LEN}\r\n").as_bytes()), Ok(None));

    let over = MAX_ARRAY_LEN + 1;
    assert!(parse(format!("*{over}\r\n").as_bytes()).is_err());
    assert_eq!(parse(format!("*{MAX_ARRAY_LEN}\r\n").as_bytes()), Ok(None));
}

#[test]
fn a_length_beyond_a_32_bit_usize_is_rejected_the_same_way_everywhere() {
    // The regression this pins: these lengths convert to `usize` on a
    // 64-bit target and fail to on a 32-bit one, so before the ceilings
    // the same bytes were a terminal error on one target and an
    // indefinite "wait for more" on the other. Both are errors now, on
    // every target, and the assertion holds wherever the suite runs.
    for header in [&b"$4294967296\r\n"[..], b"*4294967296\r\n"] {
        assert!(parse(header).is_err(), "{header:?}");
    }
}

#[test]
fn parse_error_displays_its_message() {
    let err = parse(b"!5\r\n").unwrap_err();
    assert_eq!(err.to_string(), err.0);
    assert!(err.to_string().contains("unknown RESP2 type byte"));
    // And it is a real `std::error::Error`, so a caller can box it.
    let _boxed: Box<dyn std::error::Error> = Box::new(err);
}

#[test]
fn parse_leaves_trailing_bytes_for_the_next_call() {
    let mut out = Vec::new();
    encode(&Frame::Integer(1), &mut out);
    let split = out.len();
    encode(&Frame::Integer(2), &mut out);
    let (f, used) = parse(&out).unwrap().unwrap();
    assert_eq!((f, used), (Frame::Integer(1), split));
}

/// The frames every decoder test streams. Same shapes as
/// `parse_round_trips_every_frame_type`, plus the two that only a
/// resumable decoder makes interesting: a bulk long enough to straddle
/// several chunks, and an array whose elements are individually tiny.
fn streaming_cases() -> Vec<Frame> {
    vec![
        Frame::Simple("OK".into()),
        Frame::Error("ERR boom".into()),
        Frame::Integer(-7),
        Frame::Bulk(b"hi".to_vec()),
        Frame::Bulk(Vec::new()),
        Frame::Bulk(b"a\r\nb\x00c\xffd".to_vec()),
        Frame::Null,
        Frame::Array(vec![
            Frame::Bulk(b"GET".to_vec()),
            Frame::Bulk(b"k".to_vec()),
        ]),
        Frame::Array(vec![]),
        Frame::Array(vec![Frame::Array(vec![Frame::Integer(1)])]),
        Frame::Array(vec![
            Frame::Simple("nested".into()),
            Frame::Array(vec![Frame::Null, Frame::Array(vec![])]),
            Frame::Integer(i64::MIN),
        ]),
        Frame::Bulk(vec![b'q'; 5000]),
    ]
}

/// Feeds `wire` to a fresh decoder in `chunk`-sized pieces, draining every
/// frame that becomes available after each piece.
fn drain_in_chunks(wire: &[u8], chunk: usize) -> Result<Vec<Frame>, ParseError> {
    let mut decoder = Decoder::new(DecoderLimits::default());
    let mut frames = Vec::new();
    for piece in wire.chunks(chunk) {
        decoder.feed(piece);
        while let Some(frame) = decoder.try_next()? {
            frames.push(frame);
        }
    }
    assert_eq!(decoder.buffered(), 0, "wire fully consumed");
    Ok(frames)
}

#[test]
fn decoder_equals_parse_under_every_chunking() {
    for frame in streaming_cases() {
        let mut wire = Vec::new();
        encode(&frame, &mut wire);
        let one_shot = parse(&wire).unwrap().unwrap();
        assert_eq!(one_shot, (frame.clone(), wire.len()));

        for chunk in [1, 2, 3, 7, wire.len()] {
            let frames = drain_in_chunks(&wire, chunk).unwrap();
            assert_eq!(frames, vec![frame.clone()], "chunk size {chunk}");
        }
    }

    // The same property for a pipeline: one stream carrying every case
    // back to back, which is what a real connection delivers.
    let cases = streaming_cases();
    let mut wire = Vec::new();
    for frame in &cases {
        encode(frame, &mut wire);
    }
    for chunk in [1, 2, 3, 7, wire.len()] {
        let frames = drain_in_chunks(&wire, chunk).unwrap();
        assert_eq!(frames, cases, "chunk size {chunk}");
    }
}

#[test]
fn an_unknown_type_byte_is_refused_before_a_terminator_is_waited_for() {
    // Nothing that arrives later can make these bytes a frame, so the
    // refusal must not be contingent on a `\r\n` ever showing up. If it
    // were, one junk byte would buy a peer the right to have the server
    // buffer up to `max_frame_bytes` on its behalf — the cheapest
    // amplification there is, and it would be bought for free.
    for junk in [&b"!"[..], b"!5", b"P", b"GET k", b"\0", b"HELLO world"] {
        assert!(parse(junk).is_err(), "one-shot: {junk:?}");

        let mut decoder = Decoder::new(DecoderLimits::default());
        decoder.feed(junk);
        assert!(decoder.try_next().is_err(), "streaming: {junk:?}");

        // And byte by byte: the error lands on the first byte, not once
        // the rest of the junk has been accumulated.
        let mut decoder = Decoder::new(DecoderLimits::default());
        decoder.feed(&junk[..1]);
        let err = decoder.try_next().unwrap_err();
        assert!(
            err.to_string().starts_with("unknown RESP2 type byte"),
            "unexpected error for {junk:?}: {err}"
        );
    }

    // The five bytes that *are* valid keep waiting, so the guard rejects
    // nothing it should accept.
    for opener in [&b"+"[..], b"-", b":", b"$", b"*"] {
        assert_eq!(parse(opener), Ok(None), "{opener:?}");
    }
}

/// An empty line between two top-level frames is not a frame: Redis
/// 7.4.11 answers `+PONG` to `\r\n*1\r\n$4\r\nPING\r\n`, and `redis-cli
/// --pipe` (8.10.0) writes `\r\n` between the last command of a transfer
/// and the `ECHO` that closes it. The skipped bytes count as consumed, so the
/// caller's cursor lands on the frame that followed them.
#[test]
fn an_empty_line_between_frames_is_skipped() {
    let wire = b"\r\n*1\r\n$4\r\nPING\r\n";
    assert_eq!(
        parse(wire),
        Ok(Some((
            Frame::Array(vec![Frame::Bulk(b"PING".to_vec())]),
            wire.len()
        )))
    );
    // Several in a row, and one after a frame: each is skipped on its own.
    let wire = b"\r\n\r\n+OK\r\n\r\n:1\r\n";
    let (first, used) = parse(wire).unwrap().unwrap();
    assert_eq!(first, Frame::Simple("OK".into()));
    assert_eq!(&wire[used..], b"\r\n:1\r\n");
    let (second, used2) = parse(&wire[used..]).unwrap().unwrap();
    assert_eq!(second, Frame::Integer(1));
    assert_eq!(used + used2, wire.len());
}

/// The skip is a property of the frame boundary, not of the bytes: a
/// `\r\n` where an array element is due is still an unknown type byte —
/// Redis 7.4.11 refuses the same bytes as a protocol error and closes — and
/// a lone `\r` at the end of the input waits for the byte after it.
#[test]
fn an_empty_line_inside_an_array_is_still_refused() {
    let err = parse(b"*2\r\n\r\n$1\r\na\r\n").unwrap_err();
    assert!(
        err.to_string().starts_with("unknown RESP2 type byte: 0x0d"),
        "{err}"
    );
    assert_eq!(parse(b"\r"), Ok(None));
    assert_eq!(
        parse(b"+OK\r\n\r"),
        Ok(Some((Frame::Simple("OK".into()), 5)))
    );
    // A `\r` followed by anything but `\n` is a type byte, and an unknown one.
    let err = parse(b"\rx").unwrap_err();
    assert!(
        err.to_string().starts_with("unknown RESP2 type byte: 0x0d"),
        "{err}"
    );
}

/// The skip survives every chunking: a `\r` that arrives alone waits for
/// its `\n`, and the frame behind the empty line is delivered whole.
#[test]
fn an_empty_line_is_skipped_under_every_chunking() {
    let wire = b"\r\n*1\r\n$4\r\nPING\r\n\r\n+OK\r\n";
    let want = vec![
        Frame::Array(vec![Frame::Bulk(b"PING".to_vec())]),
        Frame::Simple("OK".into()),
    ];
    for chunk in 1..=wire.len() {
        assert_eq!(drain_in_chunks(wire, chunk).unwrap(), want, "chunk={chunk}");
    }
}

#[test]
fn parse_refuses_an_incomplete_frame_past_the_buffering_limit() {
    // `parse` runs on `DecoderLimits::default()`, so the one-shot path
    // carries the same ceilings the streaming one does. This is new
    // behaviour and the boundary is worth pinning: every shorter prefix
    // of this frame is `Ok(None)`, and one byte more is terminal.
    let limit = DecoderLimits::default().max_frame_bytes;
    let mut buf = vec![b'x'; limit];
    buf[0] = b'+'; // a simple string whose terminator never comes
    assert_eq!(parse(&buf), Ok(None), "at the ceiling: still waiting");

    buf.push(b'x');
    let err = parse(&buf).unwrap_err();
    assert!(
        err.to_string().starts_with("frame exceeds"),
        "unexpected error: {err}"
    );
}

#[test]
fn parse_refuses_a_complete_frame_whose_parsed_form_is_too_large() {
    // Distinct from the ceiling above, and the easier one to miss: these
    // bytes are a complete, well-formed frame. No bulk breaks
    // `MAX_BULK_LEN`, no array breaks `MAX_ARRAY_LEN`, and nesting is two
    // deep. Only the sum is too much — which is the whole point of
    // bounding the parsed form rather than the bytes read.
    let budget = DecoderLimits::default().max_in_memory;
    let per_array = budget / (2 * size_of::<Frame>());
    assert!(per_array <= MAX_ARRAY_LEN, "each array is a legal length");

    // Integers are the cheapest way to reach the budget: 4 bytes on the
    // wire, a whole `Frame` in memory. Two arrays of `per_array` of them
    // is ~8 MiB of input and ~64 MiB parsed.
    let mut wire = b"*2\r\n".to_vec();
    for _ in 0..2 {
        wire.extend_from_slice(format!("*{per_array}\r\n").as_bytes());
        wire.extend_from_slice(&b":1\r\n".repeat(per_array));
    }
    let err = parse(&wire).unwrap_err();
    // Refused at the *second* array's header, not element by element:
    // that header is priced against what is left of the budget after its
    // sibling spent half of it, so the count alone is already unaffordable
    // by the time it is read.
    assert!(
        err.to_string()
            .starts_with(&format!("array of {per_array} elements exceeds")),
        "unexpected error: {err}"
    );

    // The neighbouring accepting case, so the assertion above cannot be
    // satisfied by a limit that rejects everything: four elements fewer
    // leaves room for the three array nodes and parses.
    let per_array = per_array - 4;
    let mut wire = b"*2\r\n".to_vec();
    for _ in 0..2 {
        wire.extend_from_slice(format!("*{per_array}\r\n").as_bytes());
        wire.extend_from_slice(&b":1\r\n".repeat(per_array));
    }
    let (frame, consumed) = parse(&wire).unwrap().unwrap();
    assert_eq!(consumed, wire.len());
    let Frame::Array(outer) = frame else {
        panic!("expected an array")
    };
    assert_eq!(outer.len(), 2);
}

/// Feeds `wire` in `chunk`-sized pieces and reports the two things
/// [`Decoder::feed`] promises are chunk-independent: the frames produced,
/// and whether the bytes were accepted at all.
///
/// Deliberately *not* the error text. Which of the two limits reports a
/// refusal depends on where the boundaries fell when they are set close
/// together, and the documentation says so; asserting on the message here
/// would pin behaviour the crate does not offer.
fn verdict(wire: &[u8], limits: DecoderLimits, chunk: usize) -> Result<Vec<Frame>, ()> {
    let mut decoder = Decoder::new(limits);
    let mut frames = Vec::new();
    for piece in wire.chunks(chunk) {
        decoder.feed(piece);
        loop {
            match decoder.try_next() {
                Ok(Some(frame)) => frames.push(frame),
                Ok(None) => break,
                Err(_) => return Err(()),
            }
        }
    }
    Ok(frames)
}

#[test]
fn the_verdict_does_not_depend_on_how_the_peer_chunks() {
    // `feed` promises that chunk boundaries carry no meaning, and that
    // promise is what everything above this crate reasons with.
    //
    // The wire ceiling used to be tested only when the decoder ran out of
    // input, so a frame over the limit was accepted whenever the last
    // chunk completed it before the check could fire. It was not even
    // monotonic in chunk size: the same 103 bytes were refused at chunk
    // 65 and accepted at chunk 64 and again at chunk 103.
    let mut line = vec![b'+'];
    line.extend(std::iter::repeat_n(b'x', 100));
    line.extend_from_slice(b"\r\n");
    let mut bulk = Vec::new();
    encode(&Frame::Bulk(vec![b'w'; 80]), &mut bulk);
    let pipeline = bulk.repeat(3);

    let ample = DecoderLimits::default().max_in_memory;
    let cases: &[(&str, &[u8], DecoderLimits, bool)] = &[
        // Both limits equal, which is what a caller with one per-request
        // budget sets and what the connection layer above this crate
        // does. This is the configuration the asymmetric cases below were
        // missing, and the one where a frame can break both bounds at
        // once — the case that decides which message a peer is told.
        (
            "equal limits, over both",
            &line,
            DecoderLimits {
                max_frame_bytes: 64,
                max_in_memory: 64,
            },
            false,
        ),
        (
            "equal limits, under both",
            &line,
            DecoderLimits {
                max_frame_bytes: 200,
                max_in_memory: 200,
            },
            true,
        ),
        // And the wire ceiling on its own, which is the only way to reach
        // the completion-path check: with the limits equal the parsed
        // bound always binds first, because a frame's parsed form is
        // never smaller than its wire form.
        (
            "wire ceiling one byte under",
            &bulk,
            DecoderLimits {
                max_frame_bytes: bulk.len() - 1,
                max_in_memory: ample,
            },
            false,
        ),
        (
            "wire ceiling exact",
            &bulk,
            DecoderLimits {
                max_frame_bytes: bulk.len(),
                max_in_memory: ample,
            },
            true,
        ),
        // The ceiling is per frame, so three of them back to back at
        // exactly the ceiling are three acceptances and not one refusal.
        // A pipeline is also the only shape that can tell the completion
        // check's `scan` from the buffer's length, which are the same
        // number whenever a frame arrives alone.
        (
            "wire ceiling exact, pipelined",
            &pipeline,
            DecoderLimits {
                max_frame_bytes: bulk.len(),
                max_in_memory: ample,
            },
            true,
        ),
    ];

    for (name, wire, limits, expected_ok) in cases {
        // The whole-in-one-chunk run is the reference every split has to
        // reproduce — frames included, not just accepted-or-not.
        let reference = verdict(wire, *limits, wire.len());
        assert_eq!(
            reference.is_ok(),
            *expected_ok,
            "{name}: the reference verdict is not the one this case is for"
        );
        for chunk in 1..=wire.len() {
            assert_eq!(
                verdict(wire, *limits, chunk),
                reference,
                "{name}: chunk {chunk} disagrees with the whole"
            );
        }
    }
}

#[test]
fn decoder_work_is_linear_in_input() {
    // The property the rework exists for. `bytes_examined` counts every
    // byte the state machine looks at or copies, so re-parsing a
    // completed element from the start of the buffer shows up in it.

    // One large bulk, dribbled a byte at a time. Parsing from offset zero
    // on every chunk re-reads the length line a million times.
    let mut wire = Vec::new();
    encode(&Frame::Bulk(vec![b'x'; 1024 * 1024]), &mut wire);
    let mut decoder = Decoder::new(DecoderLimits::default());
    for byte in &wire {
        decoder.feed(std::slice::from_ref(byte));
        if let Some(frame) = decoder.try_next().unwrap() {
            assert_eq!(frame, Frame::Bulk(vec![b'x'; 1024 * 1024]));
        }
    }
    assert!(
        decoder.bytes_examined() <= 4 * wire.len(),
        "one bulk: examined {} for {} wire bytes",
        decoder.bytes_examined(),
        wire.len()
    );

    // Many tiny elements in one array — the adversarial shape, where
    // re-parsing from offset zero also re-allocates every completed
    // element and the total work is quadratic.
    let elements: Vec<Frame> = (0..20_000).map(|_| Frame::Bulk(b"k".to_vec())).collect();
    let array = Frame::Array(elements);
    let mut wire = Vec::new();
    encode(&array, &mut wire);
    let mut decoder = Decoder::new(DecoderLimits::default());
    let mut seen = 0;
    for byte in &wire {
        decoder.feed(std::slice::from_ref(byte));
        if let Some(frame) = decoder.try_next().unwrap() {
            assert_eq!(frame, array);
            seen += 1;
        }
    }
    assert_eq!(seen, 1);
    assert!(
        decoder.bytes_examined() <= 4 * wire.len(),
        "tiny elements: examined {} for {} wire bytes",
        decoder.bytes_examined(),
        wire.len()
    );

    // One enormous *line*, which is the only shape that exercises the
    // resumable CRLF search. The two rows above have length lines of nine
    // bytes and payloads reached by arithmetic, so they pass even with the
    // resume deleted; here the terminator is a megabyte away and a search
    // that restarted at the line's first byte on every chunk would be
    // quadratic — half a trillion byte comparisons against this budget.
    let text = "x".repeat(1024 * 1024);
    let simple = Frame::Simple(text);
    let mut wire = Vec::new();
    encode(&simple, &mut wire);
    let mut decoder = Decoder::new(DecoderLimits::default());
    let mut seen = 0;
    for byte in &wire {
        decoder.feed(std::slice::from_ref(byte));
        if let Some(frame) = decoder.try_next().unwrap() {
            assert_eq!(frame, simple);
            seen += 1;
        }
    }
    assert_eq!(seen, 1);
    assert!(
        decoder.bytes_examined() <= 4 * wire.len(),
        "one long line: examined {} for {} wire bytes",
        decoder.bytes_examined(),
        wire.len()
    );
}

#[test]
fn decoder_bounds_the_parsed_representation() {
    // Tiny on the wire, fat in memory: an integer element is 4 wire bytes
    // and a whole `Frame` once parsed, so an array of them amplifies by
    // eight. A bound on bytes read cannot see that; this one is on the
    // parsed representation, and there are three places it bites.
    let budget = 1024 * 1024;
    let limits = DecoderLimits {
        max_in_memory: budget,
        ..DecoderLimits::default()
    };
    let affordable = budget / size_of::<Frame>();

    // One: a count the decoder could never afford is refused at the
    // header, before a single element byte is read. `MAX_ARRAY_LEN`
    // empty `Frame`s are 32 MiB, and the budget here is 1 MiB.
    let mut decoder = Decoder::new(limits);
    decoder.feed(format!("*{MAX_ARRAY_LEN}\r\n").as_bytes());
    decoder.feed(&b":1\r\n".repeat(10));
    let err = decoder.try_next().unwrap_err();
    // Matched on the header refusal's own wording, not on the word the
    // two refusals share: otherwise deleting the header check would leave
    // this green, caught instead by the per-element charge below.
    assert!(
        err.to_string()
            .starts_with(&format!("array of {MAX_ARRAY_LEN} elements")),
        "unexpected error: {err}"
    );
    assert!(
        decoder.state.stack.is_empty(),
        "the array was never started"
    );

    // Two: a payload the budget cannot afford is refused at its header,
    // from the length the header declares — before the payload is waited
    // for, never mind copied.
    //
    // The boundary here moved deliberately. Charging the payload where it
    // is copied still refused it, but the copy is only reachable once the
    // whole payload has been buffered, so the header alone used to answer
    // "keep going" and a peer could make the decoder hold 16 MiB it had
    // already been told it could not afford. The header is the last moment
    // the refusal is free, so that is where it happens.
    let mut decoder = Decoder::new(limits);
    let huge = MAX_BULK_LEN - 1;
    decoder.feed(format!("${huge}\r\n").as_bytes());
    let err = decoder.try_next().unwrap_err();
    assert!(
        err.to_string().starts_with("decoded frame exceeds"),
        "unexpected error: {err}"
    );
    // Nothing of the payload was held: the refusal came out of ten bytes
    // of header. `buffered` is the memory claim, and the work counter is
    // the second half of it — the counter advances by a payload's length
    // only when the decoder reads that payload out, and this one it never
    // touched.
    assert!(decoder.buffered() <= 16, "held {}", decoder.buffered());
    assert!(
        decoder.bytes_examined() < huge,
        "the payload was read out before it was refused: examined {}",
        decoder.bytes_examined()
    );

    // Three: a count that fits, filled with elements that do not. The wire
    // says nothing about the payload sizes to come, so this can only be
    // caught while the array fills — and it is caught as it fills, never
    // after the fact.
    let payload = 64 * 1024;
    let count = 100;
    assert!(count * size_of::<Frame>() < budget, "the header is payable");
    let element = Frame::Bulk(vec![b'p'; payload]);
    let mut wire = format!("*{count}\r\n").into_bytes();
    for _ in 0..count {
        encode(&element, &mut wire);
    }
    let mut decoder = Decoder::new(limits);
    decoder.feed(&wire);
    let err = decoder.try_next().unwrap_err();
    assert!(
        err.to_string().starts_with("decoded frame exceeds"),
        "unexpected error: {err}"
    );

    let held = decoder.state.stack.last().map_or(0, |a| a.elements.len());
    assert!(held > 0, "elements were accepted up to the bound");
    assert!(
        held <= budget / payload,
        "held {held} elements of {payload} bytes on a {budget}-byte budget"
    );
    assert!(held < affordable, "stopped well short of the count");
}

#[test]
fn decoder_sheds_capacity_after_a_large_frame() {
    let mut wire = Vec::new();
    encode(&Frame::Bulk(vec![b'z'; 4 * 1024 * 1024]), &mut wire);
    let mut decoder = Decoder::new(DecoderLimits::default());
    decoder.feed(&wire);
    assert!(decoder.buf.capacity() > DecoderLimits::SHED);

    let frame = decoder.try_next().unwrap().unwrap();
    assert_eq!(frame, Frame::Bulk(vec![b'z'; 4 * 1024 * 1024]));
    assert_eq!(decoder.buffered(), 0);

    // The shed happens when the decoder runs out of input, not when the
    // frame leaves — the same point the buffer is compacted at, and for
    // the same reason: a caller draining a pipelined batch must not pay a
    // reallocation between one frame and the next. Every caller reaches it
    // on the call that tells it to go and read more.
    assert_eq!(decoder.try_next(), Ok(None));
    assert!(
        decoder.buf.capacity() <= DecoderLimits::SHED,
        "capacity {} still held after draining a 4 MiB frame",
        decoder.buf.capacity()
    );
}

#[test]
fn decoder_compacts_once_per_batch_not_once_per_frame() {
    // The regression this pins is not a wrong answer, it is a quadratic:
    // every removal from the front of the buffer moves everything after
    // it, so compacting per frame makes draining a pipelined read cost
    // about `bytes × frames / 2` in memory traffic instead of `bytes`.
    let mut one = Vec::new();
    encode(&Frame::Array(vec![Frame::Bulk(b"PING".to_vec())]), &mut one);
    let count = 64;
    let batch = one.repeat(count);

    let mut decoder = Decoder::new(DecoderLimits::default());
    decoder.feed(&batch);
    for taken in 1..=count {
        let frame = decoder.try_next().unwrap().expect("a frame per repeat");
        assert_eq!(frame, Frame::Array(vec![Frame::Bulk(b"PING".to_vec())]));
        // Nothing has been moved yet: the delivered frames' bytes are
        // still sitting in front of the cursor.
        assert_eq!(
            decoder.buf.len(),
            batch.len(),
            "the buffer was compacted after frame {taken}"
        );
        // And the count of what is still owed is right anyway.
        assert_eq!(decoder.buffered(), batch.len() - taken * one.len());
    }

    // The batch is spent, so the call that reports it also reclaims it.
    assert_eq!(decoder.try_next(), Ok(None));
    assert_eq!(decoder.buf.len(), 0);
    assert_eq!(decoder.buffered(), 0);

    // A partial frame trailing the batch is compacted just the same: what
    // is dropped is bounded by where the unfinished frame starts, not by
    // whether one is in flight. Otherwise a peer that always leaves a few
    // bytes over would keep every delivered frame's bytes alive with it.
    let mut decoder = Decoder::new(DecoderLimits::default());
    decoder.feed(&batch);
    decoder.feed(&one[..3]);
    for _ in 0..count {
        assert!(decoder.try_next().unwrap().is_some());
    }
    assert_eq!(decoder.try_next(), Ok(None));
    assert_eq!(decoder.buf.len(), 3, "only the partial frame is left");
    assert_eq!(decoder.buffered(), 3);

    // ...and it still completes, from the rebased offsets.
    decoder.feed(&one[3..]);
    assert_eq!(
        decoder.try_next(),
        Ok(Some(Frame::Array(vec![Frame::Bulk(b"PING".to_vec())])))
    );
}

/// Takes a [`Frame`] apart with an explicit stack.
///
/// `Frame`'s derived `Drop` recurses once per nesting level, so letting a
/// deeply nested value fall out of scope runs the drop glue that many
/// frames deep and can overflow the test thread's stack — a failure with
/// nothing to do with the code under test. `Debug` and `PartialEq` are
/// recursive for the same reason, which is why the caller neither formats
/// nor compares the value.
fn dismantle(frame: Frame) {
    let mut stack = vec![frame];
    while let Some(frame) = stack.pop() {
        if let Frame::Array(children) = frame {
            stack.extend(children);
        }
    }
}

#[test]
fn encoding_a_deeply_nested_frame_cannot_overflow_the_stack() {
    // 10_000 levels: far past what the parser accepts, and far past what
    // a recursive encoder survives.
    let mut frame = Frame::Integer(1);
    for _ in 0..10_000 {
        frame = Frame::Array(vec![frame]);
    }

    let mut wire = Vec::new();
    encode(&frame, &mut wire);
    // Encoding returned at all — that is the first property. The length
    // is the arithmetic check that it encoded the whole structure.
    assert_eq!(wire.len(), 10_000 * b"*1\r\n".len() + b":1\r\n".len());

    // And the second: the decoder refuses those bytes at the depth limit,
    // so the asymmetry is safe in the only direction that matters.
    let err = parse(&wire).unwrap_err();
    assert!(err.to_string().contains("depth limit"), "{err}");
    let mut decoder = Decoder::new(DecoderLimits::default());
    decoder.feed(&wire);
    assert!(decoder.try_next().is_err());

    dismantle(frame);
}
