use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    thread,
    time::Duration,
};

use mc_launcher_core::{
    error::LauncherError,
    io::hash::{sha1_file, sha1_file_async},
    net::download::{execute_plan_async, Checksum, DownloadPlan, DownloadTask},
    progress::{ProgressEvent, SkipReason},
};

const BODY: &[u8] = b"async download body 0123456789";

fn spawn_body_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();

            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            while let Ok(read) = stream.read(&mut chunk) {
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                BODY.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(BODY).unwrap();
        }
    });
    address
}

fn expected_sha1() -> String {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("body.bin");
    fs::write(&path, BODY).unwrap();
    sha1_file(&path).unwrap()
}

#[tokio::test]
async fn downloads_files_concurrently_async() {
    let address = spawn_body_server();
    let base = format!("http://{address}/file.bin");
    let dir = tempfile::tempdir().unwrap();
    let checksum = expected_sha1();

    let plan = DownloadPlan {
        tasks: vec![
            DownloadTask {
                url: base.clone(),
                destination: dir.path().join("a.bin"),
                checksum: Some(Checksum::Sha1(checksum.clone())),
                label: "a".to_string(),
            },
            DownloadTask {
                url: base,
                destination: dir.path().join("b.bin"),
                checksum: Some(Checksum::Sha1(checksum)),
                label: "b".to_string(),
            },
        ],
    };

    let mut reporter = |_event: ProgressEvent| {};
    execute_plan_async(&plan, 4, &mut reporter).await.unwrap();

    assert_eq!(
        fs::read(dir.path().join("a.bin")).unwrap(),
        BODY,
        "first download content"
    );
    assert_eq!(
        fs::read(dir.path().join("b.bin")).unwrap(),
        BODY,
        "second download content"
    );
}

#[tokio::test]
async fn reports_skipped_tasks_before_downloading() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("existing.bin");
    fs::write(&destination, BODY).unwrap();

    let plan = DownloadPlan {
        tasks: vec![DownloadTask {
            url: "http://127.0.0.1:1/never-hit.bin".to_string(),
            destination,
            checksum: Some(Checksum::Sha1(expected_sha1())),
            label: "existing".to_string(),
        }],
    };

    let mut events = Vec::new();
    let mut reporter = |event: ProgressEvent| events.push(event);
    execute_plan_async(&plan, 4, &mut reporter).await.unwrap();

    let skipped = events
        .into_iter()
        .filter_map(|event| match event {
            ProgressEvent::TaskSkipped { reason, .. } => Some(reason),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(skipped, vec![SkipReason::ChecksumMatched]);
}

#[tokio::test]
async fn fails_when_checksum_mismatches() {
    let address = spawn_body_server();
    let dir = tempfile::tempdir().unwrap();

    let plan = DownloadPlan {
        tasks: vec![DownloadTask {
            url: format!("http://{address}/bad.bin"),
            destination: dir.path().join("bad.bin"),
            checksum: Some(Checksum::Sha1("0".repeat(40))),
            label: "bad".to_string(),
        }],
    };

    let mut reporter = |_event: ProgressEvent| {};
    let err = execute_plan_async(&plan, 4, &mut reporter)
        .await
        .unwrap_err();

    match err {
        LauncherError::ChecksumMismatch { .. } => {}
        other => panic!("expected checksum mismatch, got: {other}"),
    }
}

#[tokio::test]
async fn rejects_zero_workers() {
    let dir = tempfile::tempdir().unwrap();
    let plan = DownloadPlan {
        tasks: vec![DownloadTask {
            url: "http://127.0.0.1:1/never-hit.bin".to_string(),
            destination: dir.path().join("x.bin"),
            checksum: None,
            label: "x".to_string(),
        }],
    };

    let mut reporter = |_event: ProgressEvent| {};
    let err = execute_plan_async(&plan, 0, &mut reporter)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("worker count"));
}

#[tokio::test]
async fn async_sha1_matches_sync_sha1() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.bin");
    fs::write(&path, BODY).unwrap();

    assert_eq!(
        sha1_file(&path).unwrap(),
        sha1_file_async(&path).await.unwrap()
    );
}
