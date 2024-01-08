use std::fmt;
use std::ops;

/// A single temperature, with tenths of a degree precision
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Temperature {
    tenths: i32,
}

impl fmt::Debug for Temperature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Temperature({self})")
    }
}

impl fmt::Display for Temperature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sign = if self.tenths.is_negative() { "-" } else { "" };
        let whole = self.tenths.abs() / 10;
        let frac = self.tenths.abs() % 10;
        write!(f, "{sign}{whole}.{frac}")
    }
}

impl Temperature {
    /// Parse an ASCII string of the form `-?[0-9]+\.[0-9]`
    pub fn parse(s: impl AsRef<[u8]>) -> Result<Self, &'static str> {
        #[derive(Clone, Copy, PartialEq)]
        enum State {
            Sign,
            Digit,
            Frac,
            Done,
        }

        let s = s.as_ref();
        let mut negative = false;
        let mut state = State::Sign;
        let mut tenths = 0_i32;

        for b in s.iter().copied() {
            match (state, b) {
                (State::Sign, b'-') => {
                    negative = true;
                    state = State::Digit;
                }
                (State::Sign, d @ b'0'..=b'9') => {
                    tenths = (d - b'0') as i32;
                    state = State::Digit;
                }
                (State::Sign, _) => return Err("invalid character"),

                (State::Digit, d @ b'0'..=b'9') => tenths = (tenths * 10) + (d - b'0') as i32,
                (State::Digit, b'.') => state = State::Frac,
                (State::Digit, _) => return Err("invalid character"),

                (State::Frac, d @ b'0'..=b'9') => {
                    tenths = (tenths * 10) + (d - b'0') as i32;
                    state = State::Done;
                }
                (State::Frac, _) => return Err("invalid character"),

                (State::Done, _) => return Err("trailing characters"),
            }
        }

        if state != State::Done {
            return Err("truncated input");
        }

        if negative {
            tenths = -tenths;
        }

        Ok(Self { tenths })
    }
}

// hand-rolled ops implementations. Just the ones I actually use, not trying to be fully complete

impl ops::AddAssign for Temperature {
    fn add_assign(&mut self, rhs: Temperature) {
        self.tenths += rhs.tenths;
    }
}

impl ops::Div<u32> for Temperature {
    type Output = Temperature;

    fn div(self, rhs: u32) -> Self::Output {
        Temperature {
            tenths: ((self.tenths as f64) / (rhs as f64)).round() as i32,
        }
    }
}

#[cfg(test)]
#[test]
fn test_temperature() {
    assert_eq!(Temperature::parse("12.3"), Ok(Temperature { tenths: 123 }));
    assert_eq!(Temperature::parse("-0.1"), Ok(Temperature { tenths: -1 }));
    assert_eq!(
        Temperature::parse("123456789.0"),
        Ok(Temperature { tenths: 1234567890 })
    );
    assert!(Temperature::parse("").is_err());
    assert!(Temperature::parse("12345.6 ").is_err());
    assert!(Temperature::parse("foo0.1").is_err());
    assert!(Temperature::parse("-123").is_err());

    // values should round-trip through parse and display, and ensures that the modular arithmetic
    // in Display is correct.
    let nums = ["0.0", "1.0", "123.5", "-1.0", "-1.4", "-0.2", "-100.3"];
    for s in nums {
        let t = Temperature::parse(s).unwrap();
        assert_eq!(s, t.to_string().as_str());
    }
}
