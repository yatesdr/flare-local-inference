//! The link between rank 0 of a multi-process instance and each peer rank's agent.
//!
//! The messages are defined in `proto/rank_link.proto` and are internal to this crate: the
//! host (Flare) moves frames as opaque bytes through [`FrameSender`] and [`FrameReceiver`].
//! A frame is one `LinkMessage`.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};

use prost::Message;
use tokio::sync::Notify;

use crate::{Error, Result};

/// Messages of the rank link.
pub(crate) mod link {
    tonic::include_proto!("selene.link");
}

pub(crate) use link::link_message::M as Msg;

/// A frame larger than this is a protocol error.
pub const MAX_FRAME: usize = 8 << 20;
/// Frames queued for sending before the link is considered stuck.
const OUTBOUND: usize = 1024;
/// The reader wakes this often to notice a stop request.
const POLL: Duration = Duration::from_millis(50);

/// The sending half of a link to a peer rank. One frame per call, in order.
pub trait FrameSender: Send + 'static {
    /// Queue or write one frame. `Err` means the link is unusable and stays so.
    fn send(&mut self, frame: Vec<u8>) -> Result<()>;
}

/// The receiving half of a link.
pub trait FrameReceiver: Send + 'static {
    /// The next frame, waiting at most `timeout`. `Ok(None)` is a timeout. `Err` means
    /// the link is closed or failed and stays so.
    fn recv(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>>;
}

/// A connected link to one peer rank, as the host hands it over.
pub struct RankLink {
    pub sender: Box<dyn FrameSender>,
    pub receiver: Box<dyn FrameReceiver>,
}

/// The ranks of a multi-process instance, from rank 0's side.
pub struct RankSet {
    /// `links[i]` reaches rank `i + 1`.
    pub links: Vec<RankLink>,
}

pub(crate) fn encode(message: Msg) -> Vec<u8> {
    link::LinkMessage { m: Some(message) }.encode_to_vec()
}

pub(crate) fn decode(frame: &[u8]) -> Result<Msg> {
    if frame.len() > MAX_FRAME {
        return Err(Error::new("a rank link frame is too large"));
    }
    link::LinkMessage::decode(frame)
        .map_err(|error| Error::new(format!("malformed rank link frame: {error}")))?
        .m
        .ok_or_else(|| Error::new("an empty rank link frame"))
}

/// What arrived on a link.
#[derive(Debug)]
pub(crate) enum Incoming {
    Message(Msg),
    /// The link ended or failed; nothing follows.
    Closed(String),
}

/// A link with its two I/O threads. Sending never blocks the caller: a frame is queued, and
/// a full queue or a failed link makes [`send`](Self::send) return false for good.
pub(crate) struct LinkEnd {
    outbox: mpsc::SyncSender<Vec<u8>>,
    inbox: Arc<Mutex<VecDeque<Incoming>>>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl LinkEnd {
    /// Start the threads. `wake` is notified whenever something arrives.
    pub(crate) fn start(link: RankLink, wake: Arc<Notify>) -> std::io::Result<Self> {
        let (outbox, queued) = mpsc::sync_channel::<Vec<u8>>(OUTBOUND);
        let inbox = Arc::new(Mutex::new(VecDeque::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicBool::new(false));
        let RankLink {
            mut sender,
            mut receiver,
        } = link;
        let writer = {
            let (stop, failed, inbox, wake) =
                (stop.clone(), failed.clone(), inbox.clone(), wake.clone());
            std::thread::Builder::new()
                .name("selene-rank-send".into())
                .spawn(move || {
                    loop {
                        match queued.recv_timeout(POLL) {
                            Ok(frame) => {
                                if let Err(error) = sender.send(frame) {
                                    if !failed.swap(true, Ordering::SeqCst) {
                                        inbox
                                            .lock()
                                            .unwrap()
                                            .push_back(Incoming::Closed(error.to_string()));
                                        wake.notify_one();
                                    }
                                    return;
                                }
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => {
                                if stop.load(Ordering::SeqCst) {
                                    return;
                                }
                            }
                            Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        }
                    }
                })?
        };
        let reader = {
            let (stop, failed, inbox) = (stop.clone(), failed.clone(), inbox.clone());
            std::thread::Builder::new()
                .name("selene-rank-recv".into())
                .spawn(move || {
                    while !stop.load(Ordering::SeqCst) {
                        match receiver.recv(POLL) {
                            Ok(None) => {}
                            Ok(Some(frame)) => {
                                let entry = match decode(&frame) {
                                    Ok(message) => Incoming::Message(message),
                                    Err(error) => {
                                        failed.store(true, Ordering::SeqCst);
                                        let mut queue = inbox.lock().unwrap();
                                        queue.push_back(Incoming::Closed(error.to_string()));
                                        drop(queue);
                                        wake.notify_one();
                                        return;
                                    }
                                };
                                inbox.lock().unwrap().push_back(entry);
                                wake.notify_one();
                            }
                            Err(error) => {
                                if !failed.swap(true, Ordering::SeqCst) {
                                    inbox
                                        .lock()
                                        .unwrap()
                                        .push_back(Incoming::Closed(error.to_string()));
                                    wake.notify_one();
                                }
                                return;
                            }
                        }
                    }
                })?
        };
        Ok(Self {
            outbox,
            inbox,
            stop,
            failed,
            threads: vec![writer, reader],
        })
    }

    /// Queue a message. False when the link is closed, failed or stuck.
    pub(crate) fn send(&self, message: Msg) -> bool {
        if self.failed.load(Ordering::SeqCst) {
            return false;
        }
        let frame = encode(message);
        match self.outbox.try_send(frame) {
            Ok(()) => true,
            Err(_) => {
                self.failed.store(true, Ordering::SeqCst);
                false
            }
        }
    }

    pub(crate) fn poll(&self, out: &mut Vec<Incoming>) {
        out.extend(self.inbox.lock().unwrap().drain(..));
    }

    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    /// Ask the threads to end and wait for them.
    pub(crate) fn close(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for LinkEnd {
    fn drop(&mut self) {
        self.close();
    }
}

/// Two connected in-memory links, for tests. Closing one end's sender makes the other end's
/// receiver fail.
#[cfg(any(test, feature = "test-support"))]
pub fn memory_links() -> (RankLink, RankLink) {
    let (a_tx, a_rx) = mpsc::channel::<Vec<u8>>();
    let (b_tx, b_rx) = mpsc::channel::<Vec<u8>>();
    struct Tx(mpsc::Sender<Vec<u8>>);
    impl FrameSender for Tx {
        fn send(&mut self, frame: Vec<u8>) -> Result<()> {
            self.0
                .send(frame)
                .map_err(|_| Error::new("the link is closed"))
        }
    }
    struct Rx(mpsc::Receiver<Vec<u8>>);
    impl FrameReceiver for Rx {
        fn recv(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
            match self.0.recv_timeout(timeout) {
                Ok(frame) => Ok(Some(frame)),
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
                Err(mpsc::RecvTimeoutError::Disconnected) => Err(Error::new("the link is closed")),
            }
        }
    }
    (
        RankLink {
            sender: Box::new(Tx(a_tx)),
            receiver: Box::new(Rx(b_rx)),
        },
        RankLink {
            sender: Box::new(Tx(b_tx)),
            receiver: Box::new(Rx(a_rx)),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait(end: &LinkEnd, want: usize) -> Vec<Incoming> {
        let mut got = Vec::new();
        for _ in 0..200 {
            end.poll(&mut got);
            if got.len() >= want {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        got
    }

    #[test]
    fn frames_round_trip_in_order_and_bad_frames_fail_the_link() {
        let (a, b) = memory_links();
        let wake = Arc::new(Notify::new());
        let a = LinkEnd::start(a, wake.clone()).unwrap();
        let b = LinkEnd::start(b, wake).unwrap();
        for xfer in 0..5 {
            assert!(a.send(Msg::Cancel(link::Cancel { xfer })));
        }
        let got = wait(&b, 5);
        let xfers: Vec<u64> = got
            .iter()
            .map(|m| match m {
                Incoming::Message(Msg::Cancel(c)) => c.xfer,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(xfers, [0, 1, 2, 3, 4]);
        assert!(decode(&[0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode(&[]).is_err());
    }

    #[test]
    fn a_closed_peer_is_reported_once_and_sends_then_fail() {
        let (a, b) = memory_links();
        let wake = Arc::new(Notify::new());
        let a = LinkEnd::start(a, wake.clone()).unwrap();
        let b = LinkEnd::start(b, wake).unwrap();
        drop(b);
        let got = wait(&a, 1);
        assert!(matches!(got.as_slice(), [Incoming::Closed(_)]), "{got:?}");
        assert!(a.failed());
        // The queue may accept a frame before the writer notices; the failure is sticky.
        for _ in 0..50 {
            a.send(Msg::Close(link::Close {}));
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!a.send(Msg::Close(link::Close {})));
        let again = wait(&a, 1);
        assert!(again.is_empty());
    }
}
