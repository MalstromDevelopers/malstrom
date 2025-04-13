use crate::{
    channels::operator_io::Output,
    operators::{map::Map, split::Split},
    stream::StreamBuilder,
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
    mixed: StreamBuilder<K, OnTimeLate<V>, T>,
) -> (StreamBuilder<K, V, T>, StreamBuilder<K, V, T>) {
    // create a randint so we do not get name collisions.
    // u32 because unlick u64 it works well when displayed in a
    // browser (floats only)
    let randint = rand::random::<u32>();
    let [ontime, late] = mixed.const_split::<2>(
        &format!("malstrom::time-split-{randint}"),
        |x, outs| match x.value {
            OnTimeLate::OnTime(_) => {
                outs[0] = true;
            }
            OnTimeLate::Late(_) => {
                outs[1] = true;
            }
        },
    );
    let ontime = ontime.map(&format!("malstrom::ontime-{randint}"), |x| match x {
        OnTimeLate::OnTime(y) => y,
        OnTimeLate::Late(_) => unreachable!("ontime"),
    });

    let late = late.map(&format!("malstrom::late-{randint}"), |x| match x {
        OnTimeLate::OnTime(_) => unreachable!("late"),
        OnTimeLate::Late(y) => y,
    });
    (ontime, late)
}
