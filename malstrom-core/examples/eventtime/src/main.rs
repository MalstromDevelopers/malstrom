//! Example job showcasing the role of epochs in tracking event time
use chrono::{DateTime, Datelike as _, NaiveDate, NaiveDateTime, NaiveTime, Weekday};
use fake::{Dummy, Fake, Faker};
use indexmap::IndexMap;
use malstrom::{
    channels::operator_io::Output, keyed::partitioners::rendezvous_select, operators::*, runtime::SingleThreadRuntime, sinks::{StatelessSink, StdOutSink}, snapshot::NoPersistence, sources::{SingleIteratorSource, StatelessSource}, types::{DataMessage, Message, Timestamp}, worker::StreamProvider
};
use serde::{Deserialize, Serialize};

type AccountId = usize;
/// Fake transaction to track
#[derive(Debug, Dummy, Clone, Serialize, Deserialize)]
pub struct Transaction {
    #[dummy(faker = "0..5")]
    account_id: AccountId,
    #[dummy(faker = "-1000.0..1000.0")]
    amount: f32,
    transaction_time: TransactionTime,
}

fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap();
}

fn build_dataflow(provider: &mut dyn StreamProvider) {
    let data = std::iter::from_fn(|| Some(Faker.fake::<Transaction>())).take(1000);
    let latest_timestamp = NaiveDateTime::MIN;

    let (stream, _) = provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(data)),
        )
        .key_distribute("key-account", |msg| msg.value.account_id, rendezvous_select)
        .assign_timestamps("assign-time", |msg| msg.value.transaction_time.clone())
        .generate_epochs("end-of-week", move |msg, _| {
            // issue an epoch everytime we advance a week
            let timestamp = msg.timestamp.0;

            if (timestamp - latest_timestamp).num_weeks() > 0 {
                // emit the start of the week as an epoch
                let start_of_week = NaiveDate::from_isoywd_opt(
                    timestamp.year(),
                    timestamp.iso_week().week(),
                    Weekday::Mon,
                )
                .unwrap()
                .and_time(NaiveTime::default());
                Some(TransactionTime(start_of_week));
            }
            None
        });

    stream
        .stateful_op("transaction-counter", TransactionCounter)
        .sink("stdout", StatelessSink::new(StdOutSink));
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct TransactionTime(NaiveDateTime);

impl fake::Dummy for TransactionTime {
	fn dummy_with_rng<R: fake::Rng + ?Sized>(config: &T, rng: &mut R) -> Self {
		todo!()
	}
}

impl Timestamp for TransactionTime {
    const MAX: Self = Self(NaiveDateTime::MAX);

    const MIN: Self = Self(NaiveDateTime::MIN);

    fn merge(&self, other: &Self) -> Self {
        let lower = self.0.min(other.0);
        Self(lower)
    }
}

struct TransactionCounter;

impl StatefulLogic<AccountId, Transaction, TransactionTime, f32, f32> for TransactionCounter {
    fn on_data(
        &mut self,
        msg: DataMessage<AccountId, Transaction, TransactionTime>,
        key_state: f32,
        _output: &mut Output<AccountId, f32, TransactionTime>,
    ) -> Option<f32> {
        // update the balance
        Some(key_state + msg.value.amount)
    }

    /// At the end of every week emit and reset the balance
    fn on_epoch(
        &mut self,
        epoch: &TransactionTime,
        state: &mut IndexMap<AccountId, f32>,
        output: &mut Output<AccountId, f32, TransactionTime>,
    ) {
        // draining resets all accounts since the state will default to 0.0
        for (account, balance) in state.drain(..) {
            output.send(Message::Data(DataMessage::new(
                account,
                balance,
                epoch.clone(),
            )));
        }
    }
}
