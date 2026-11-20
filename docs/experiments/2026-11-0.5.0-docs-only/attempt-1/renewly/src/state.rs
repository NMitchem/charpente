//! A subscription's states and the one table of transitions between them.
//! Every route and job that moves a subscription asks [`step`], so a refusal
//! always names the transition, the state, and what that state does allow.

use charpente::core::table::Codec;
use std::io;

/// Where a subscription stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubState {
    /// Paid for the current period.
    Active,
    /// The period ended; the renewal charge was sent and has not settled.
    Renewing,
    /// The renewal charge was given up.
    PastDue,
    /// The customer cancelled: access until the period ends, never charged again.
    Cancelled,
}

/// A move from one state to another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// The customer cancels before the period ends.
    Cancel,
    /// The period ended, so the renewal charge is sent.
    Renew,
    /// The renewal charge settled: a new period starts.
    Settle,
    /// The renewal charge was given up.
    GiveUp,
    /// The owner asks for a past-due subscription to be charged again.
    Retry,
}

impl SubState {
    /// The state's name, as the wire and every refusal spell it.
    pub fn name(self) -> &'static str {
        match self {
            SubState::Active => "active",
            SubState::Renewing => "renewing",
            SubState::PastDue => "past_due",
            SubState::Cancelled => "cancelled",
        }
    }

    /// The transitions this state allows, in the order a refusal lists them.
    pub fn allowed(self) -> &'static [Transition] {
        match self {
            SubState::Active => &[Transition::Cancel, Transition::Renew],
            SubState::Renewing => &[Transition::Settle, Transition::GiveUp],
            SubState::PastDue => &[Transition::Retry],
            SubState::Cancelled => &[],
        }
    }
}

impl Transition {
    /// The transition's name, as a refusal spells it.
    pub fn name(self) -> &'static str {
        match self {
            Transition::Cancel => "cancel",
            Transition::Renew => "renew",
            Transition::Settle => "settle",
            Transition::GiveUp => "give_up",
            Transition::Retry => "retry",
        }
    }

    /// The state the transition leads to.
    pub fn target(self) -> SubState {
        match self {
            Transition::Cancel => SubState::Cancelled,
            Transition::Renew => SubState::Renewing,
            Transition::Settle => SubState::Active,
            Transition::GiveUp => SubState::PastDue,
            Transition::Retry => SubState::Renewing,
        }
    }
}

/// The refusal for `t` from `state`, naming what `state` allows.
pub fn refusal(state: SubState, t: Transition) -> String {
    let allowed: Vec<&str> = state.allowed().iter().map(|a| a.name()).collect();
    let list = if allowed.is_empty() {
        "none".to_string()
    } else {
        allowed.join(", ")
    };
    format!(
        "transition refused: cannot {} a subscription that is {}; from {} the allowed transitions are: {}",
        t.name(),
        state.name(),
        state.name(),
        list
    )
}

/// The state `t` leads to from `state`, or the refusal by name.
pub fn step(state: SubState, t: Transition) -> Result<SubState, String> {
    if state.allowed().contains(&t) {
        Ok(t.target())
    } else {
        Err(refusal(state, t))
    }
}

impl Codec for SubState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            SubState::Active => 0,
            SubState::Renewing => 1,
            SubState::PastDue => 2,
            SubState::Cancelled => 3,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(SubState::Active),
            1 => Ok(SubState::Renewing),
            2 => Ok(SubState::PastDue),
            3 => Ok(SubState::Cancelled),
            other => Err(io::Error::other(format!(
                "invalid SubState tag {other} — schema drift or corruption"
            ))),
        }
    }
}
