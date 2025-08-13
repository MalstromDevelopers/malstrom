//! Example job showcasing the role of epochs in tracking event time
use std::u8;

use chrono::{Datelike, NaiveDate, TimeDelta};
use indexmap::IndexMap;
use malstrom::{
    channels::operator_io::Output,
    keyed::partitioners::rendezvous_select,
    operators::*,
    runtime::SingleThreadRuntime,
    sinks::{StatelessSink, StdOutSink},
    snapshot::NoPersistence,
    sources::{SingleIteratorSource, StatelessSource},
    types::{DataMessage, Message, Timestamp},
    worker::StreamProvider,
};
use serde::{Deserialize, Serialize};

/// Fake transactions to track, now out of order
static TRANSACTIONS: [Transaction; 10] = [
    Transaction {
        amount: 1000.0,
        transaction_time: TransactionTime::new(2025, 1, 1),
    },
    Transaction {
        amount: -20.0,
        transaction_time: TransactionTime::new(2025, 1, 5),
    },
    Transaction {
        amount: -300.0,
        transaction_time: TransactionTime::new(2025, 2, 4),
    },
    Transaction {
        amount: -150.0,
        transaction_time: TransactionTime::new(2025, 1, 17),
    },
    Transaction {
        amount: 60.0,
        transaction_time: TransactionTime::new(2025, 2, 16),
    },
    Transaction {
        amount: -55.0,
        transaction_time: TransactionTime::new(2025, 3, 16),
    },
    Transaction {
        amount: 75.0,
        transaction_time: TransactionTime::new(2025, 2, 25),
    },
    Transaction {
        amount: -10.0,
        transaction_time: TransactionTime::new(2025, 4, 5),
    },
    Transaction {
        amount: 200.0,
        transaction_time: TransactionTime::new(2025, 3, 31),
    },
    Transaction {
        amount: -5.0,
        transaction_time: TransactionTime::new(2025, 4, 19),
    },
];

fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap();
}

fn build_dataflow(provider: &mut dyn StreamProvider) {
    let (stream, _late) = provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(TRANSACTIONS.clone())),
        )
        .key_distribute(
            "key-year-month",
            |msg| {
                let ts = msg.value.transaction_time.0;
                (ts.year(), ts.month())
            },
            rendezvous_select,
        )
        .assign_timestamps("assign-time", |msg| msg.value.transaction_time.clone())
        .generate_epochs("end-of-month", limit_out_of_orderness(TimeDelta::days(62)));

    stream
        .stateful_op("transaction-counter", TransactionCounter)
        .sink("stdout", StatelessSink::new(StdOutSink));
}
struct TransactionCounter;

impl StatefulLogic<(i32, u32), Transaction, TransactionTime, f32, f32> for TransactionCounter {
    fn on_data(
        &mut self,
        msg: DataMessage<(i32, u32), Transaction, TransactionTime>,
        key_state: f32,
        _output: &mut Output<(i32, u32), f32, TransactionTime>,
    ) -> Option<f32> {
        // update the balance
        Some(key_state + msg.value.amount)
    }

    /// At the end of every month emit and reset the balance
    fn on_epoch(
        &mut self,
        epoch: &TransactionTime,
        state: &mut IndexMap<(i32, u32), f32>,
        output: &mut Output<(i32, u32), f32, TransactionTime>,
    ) {
        // remove all closed months from state
        state.retain(|(year, month), balance| {
            if (year, month) <= (&epoch.0.year(), &epoch.0.month()) {
                output.send(Message::Data(DataMessage::new(
                    (*year, *month),
                    *balance,
                    epoch.clone(),
                )));
                false
            } else {
                // retain state
                true
            }
        });
    }
}

/// Fake transaction to track
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    amount: f32,
    transaction_time: TransactionTime,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
struct TransactionTime(NaiveDate);

impl TransactionTime {
    const fn new(year: i32, month: u32, day: u32) -> Self {
        Self(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }
}

impl Timestamp for TransactionTime {
    const MAX: Self = Self(NaiveDate::MAX);

    const MIN: Self = Self(NaiveDate::MIN);

    fn merge(&self, other: &Self) -> Self {
        Self(self.0.min(other.0))
    }
}

impl std::ops::Sub<TimeDelta> for TransactionTime {
    type Output = Self;

    fn sub(self, rhs: TimeDelta) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}
