use crate::{
    channels::selective_broadcast::Output,
    operators::map::Map,
    stream::JetStreamBuilder,
    types::{DataMessage, MaybeData, MaybeKey, Message, Timestamp},
};

use super::timed_stream::OnTimeLate;

#[inline(always)]
pub(super) fn handle_maybe_late_msg<K: MaybeKey, V: MaybeData, T: Timestamp>(
    prev_epoch: Option<&T>,
    d: DataMessage<K, V, T>,
    output: &mut Output<K, OnTimeLate<V>, T>,
) {
    let wrapped = if let Some(prev) = prev_epoch.as_ref() {
        if **prev < d.timestamp {
            OnTimeLate::OnTime(d.value)
        } else {
            OnTimeLate::Late(d.value)
        }
    } else {
        // prev epoch is None so the message can not be late
        OnTimeLate::OnTime(d.value)
    };
    output.send(Message::Data(DataMessage::new(d.key, wrapped, d.timestamp)));
}

pub(super) fn split_mixed_stream<K: MaybeKey, V: MaybeData, T: Timestamp>(
    mixed: JetStreamBuilder<K, OnTimeLate<V>, T>,
) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, T>) {
    let [ontime, late] = mixed.split_n(|x, _| match x.value {
        OnTimeLate::OnTime(_) => [0].into_iter().collect(),
        OnTimeLate::Late(_) => [1].into_iter().collect(),
    });

    let ontime = ontime.map(|x| match x {
        OnTimeLate::OnTime(y) => y,
        OnTimeLate::Late(_) => unreachable!("ontime"),
    });

    let late = late.map(|x| match x {
        OnTimeLate::OnTime(_) => unreachable!("late"),
        OnTimeLate::Late(y) => y,
    });
    (ontime, late)
}
