use std::ops::{Range, RangeInclusive};

use anyhow::{anyhow, Error, Result};
use derive_builder::Builder;
use fuzzerang::{StandardBuffered, StandardSeedableRng, TryDistribution, TryRanged};
use rand::{thread_rng, Rng, SeedableRng};
use regex_syntax::hir::{Class, Hir, HirKind};

pub trait Visitor {
    type Output;
    type Err;

    fn start(&mut self);

    fn finish(&mut self) -> Result<Self::Output, Self::Err>;

    fn visit(&mut self, _hir: &Hir, _meta: &Meta) -> Result<(), Self::Err> {
        Ok(())
    }
}

#[derive(Builder)]
pub struct RngVisitor {
    #[builder(default = "false")]
    /// Whether to fail, returning an error, when the underlying RNG is exhausted
    /// of data.
    fail_on_exhaust: bool,
    #[builder(default = "true")]
    /// Whether to fall back to using a real random number generator when the
    /// current one is exhausted. This can help when generating the output of
    /// grammars that progress infinitely under the default (first-path) strategy.
    fallback_to_thread_rng: bool,
    /// The RNG to use. This is a `StandardSeedableRng` only, for now.
    rng: StandardSeedableRng,
    #[builder(default = "StandardBuffered::new()")]
    /// The buffered distribution to use. This is a `StandardBuffered` only, for now.
    distribution: StandardBuffered,
    #[builder(default)]
    /// The progressively generated output.
    output: Vec<u8>,
}

impl RngVisitor {
    fn try_sample<T>(&mut self) -> Result<T>
    where
        StandardBuffered: TryDistribution<T>,
    {
        self.distribution.try_sample(&mut self.rng)
    }

    fn try_sample_range<T>(&mut self, range: Range<T>) -> Result<T>
    where
        StandardBuffered: TryRanged<T>,
    {
        self.distribution.try_sample_range(&mut self.rng, range)
    }

    fn try_sample_range_inclusive<T>(&mut self, range: RangeInclusive<T>) -> Result<T>
    where
        StandardBuffered: TryRanged<T>,
    {
        self.distribution
            .try_sample_range_inclusive(&mut self.rng, range)
    }
}

impl SeedableRng for RngVisitor {
    type Seed = Vec<u8>;

    fn from_seed(seed: Self::Seed) -> Self {
        Self {
            fail_on_exhaust: false,
            fallback_to_thread_rng: true,
            rng: StandardSeedableRng::from_seed(seed),
            distribution: StandardBuffered::new(),
            output: Vec::new(),
        }
    }
}

impl Visitor for RngVisitor {
    type Output = Vec<u8>;

    type Err = Error;

    fn start(&mut self) {
        self.output = Vec::new();
    }

    fn visit(&mut self, hir: &Hir, _meta: &Meta) -> Result<()> {
        match hir.kind() {
            HirKind::Empty => {}
            HirKind::Literal(lit) => {
                self.output.extend(&*lit.0);
            }
            HirKind::Class(cls) => match cls {
                Class::Unicode(ucls) => {
                    if let Some(lit) = ucls.literal() {
                        self.output.extend(&lit);
                    } else {
                        let intervals = ucls.ranges();
                        match self.try_sample_range(0..intervals.len()) {
                            Ok(idx) => {
                                let interval = intervals[idx];
                                match self
                                    .try_sample_range_inclusive(interval.start()..=interval.end())
                                {
                                    Ok(c) => {
                                        let mut buf = [0; 4];
                                        c.encode_utf8(&mut buf);
                                        self.output.extend(&buf);
                                    }
                                    Err(e) => {
                                        if self.fail_on_exhaust {
                                            return Err(e);
                                        } else {
                                            let mut buf = [0; 4];
                                            interval.start().encode_utf8(&mut buf);
                                            self.output.extend(&buf);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                if self.fallback_to_thread_rng {
                                    let interval =
                                        intervals[thread_rng().gen_range(0..intervals.len())];
                                    let mut buf = [0; 4];
                                    thread_rng()
                                        .gen_range(interval.start()..=interval.end())
                                        .encode_utf8(&mut buf);
                                    self.output.extend(&buf);
                                } else if self.fail_on_exhaust {
                                    return Err(e);
                                } else {
                                    let mut buf = [0; 4];
                                    intervals[0].start().encode_utf8(&mut buf);
                                    self.output.extend(&buf);
                                }
                            }
                        }
                    }
                }
                Class::Bytes(bcls) => {
                    if let Some(lit) = bcls.literal() {
                        self.output.extend(&lit);
                    } else {
                        let intervals = bcls.ranges();
                        match self.try_sample_range(0..intervals.len()) {
                            Ok(index) => {
                                let interval = intervals[index];
                                match self
                                    .try_sample_range_inclusive(interval.start()..=interval.end())
                                {
                                    Ok(c) => self.output.push(c),
                                    Err(e) => {
                                        if self.fail_on_exhaust {
                                            return Err(e);
                                        } else {
                                            self.output.push(interval.start());
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                if self.fallback_to_thread_rng {
                                    self.output.push({
                                        let interval =
                                            intervals[thread_rng().gen_range(0..intervals.len())];
                                        thread_rng().gen_range(interval.start()..=interval.end())
                                    });
                                } else if self.fail_on_exhaust {
                                    return Err(e);
                                } else {
                                    self.output.push(intervals[0].start());
                                }
                            }
                        }
                    }
                }
            },
            HirKind::Look(_) => todo!(),
            _ => {}
        }
        println!("{:?}", self.output);
        Ok(())
    }

    fn finish(&mut self) -> Result<Self::Output> {
        Ok(self.output.clone())
    }
}

#[derive(Default, Debug)]
pub struct Meta {
    repetitions: Option<u32>,
    concat_idx: Option<usize>,
    alternation_visited: bool,
}

pub fn visit(
    hir: &Hir,
    visitor: &mut RngVisitor,
) -> Result<<RngVisitor as Visitor>::Output, <RngVisitor as Visitor>::Err> {
    let mut stack = vec![(hir, Meta::default())];

    visitor.start();

    while let Some((top, meta)) = stack.last() {
        visitor
            .visit(top, meta)
            .map_err(|e| anyhow!("Error while visiting: {}", e))?;
        match top.kind() {
            HirKind::Repetition(rep) => {
                if let Some(repetitions) = meta.repetitions.as_ref() {
                    let repetitions = *repetitions;
                    // This isn't the first visit
                    if repetitions < rep.min
                        || (repetitions < rep.max.unwrap_or(repetitions + 1)
                            // Even if we fall back to thread RNG, we don't want to take repetitions
                            // once we exhaust our own RNG
                            && visitor.try_sample().unwrap_or(false))
                    {
                        // We may visit the subexpression again
                        let (top, meta) = stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                        stack.push((
                            top,
                            Meta {
                                repetitions: Some(repetitions + 1),
                                ..meta
                            },
                        ));
                        stack.push((&rep.sub, Meta::default()));
                    } else {
                        // We must not visit the subexpression again
                        stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                    }
                } else {
                    // This is the first visit
                    let (top, meta) = stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                    stack.push((
                        top,
                        Meta {
                            repetitions: Some(1),
                            ..meta
                        },
                    ));
                    stack.push((&rep.sub, Meta::default()))
                }
            }
            HirKind::Concat(concat) => {
                if let Some(concat_idx) = meta.concat_idx.as_ref() {
                    // Not the first visit
                    if concat_idx >= &concat.len() {
                        // We have visited all subexpressions
                        stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                    } else {
                        let idx = *concat_idx;
                        // We must visit the next subexpression
                        let (top, meta) = stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;

                        stack.push((
                            top,
                            Meta {
                                concat_idx: Some(idx + 1),
                                ..meta
                            },
                        ));

                        stack.push((&concat[idx], Meta::default()));
                    }
                } else {
                    // First visit
                    let (top, meta) = stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                    stack.push((
                        top,
                        Meta {
                            concat_idx: Some(0),
                            ..meta
                        },
                    ));
                    stack.push((&concat[0], Meta::default()))
                }
            }
            HirKind::Alternation(alt) => {
                if meta.alternation_visited {
                    // We have visited all subexpressions
                    stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                } else {
                    // We must visit the next subexpression
                    let (top, meta) = stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
                    stack.push((
                        top,
                        Meta {
                            alternation_visited: true,
                            ..meta
                        },
                    ));
                    let alternation_index = visitor.try_sample_range(0..alt.len()).unwrap_or(
                        if visitor.fallback_to_thread_rng {
                            thread_rng().gen_range(0..alt.len())
                        } else {
                            0
                        },
                    );
                    stack.push((&alt[alternation_index], Meta::default()));
                }
            }
            _ => {
                stack.pop().ok_or_else(|| anyhow!("Stack underflow"))?;
            }
        }
    }

    visitor.finish()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::Result;
    use fuzzerang::StandardSeedableRng;
    use rand::{thread_rng, Rng, SeedableRng};
    use regex_syntax::{ast::parse::Parser, hir::translate::TranslatorBuilder};

    use crate::{visit, RngVisitorBuilder, Visitor};

    fn test(regex: &str, seed: Vec<u8>, bytes: bool) -> Result<Vec<u8>> {
        println!("Testing regex: {}", regex);
        let mut p = Parser::new();
        let ast = p.parse(regex)?;
        let hir = if bytes {
            let mut t = TranslatorBuilder::new().unicode(false).utf8(false).build();
            t.translate(regex, &ast)?
        } else {
            let mut t = TranslatorBuilder::new().unicode(true).utf8(false).build();
            t.translate(regex, &ast)?
        };
        println!("HIR: {:?}", hir);
        let mut visitor = RngVisitorBuilder::default()
            .rng(StandardSeedableRng::from_seed(seed))
            .build()?;
        visit(&hir, &mut visitor)?;
        let r = visitor.finish()?;
        println!("Result: {:?}", r);
        match String::from_utf8(r.clone()) {
            Ok(s) => println!("Result (string): {}", s),
            Err(e) => eprintln!("Result (invalid UTF-8): {:?}", e),
        }
        Ok(r)
    }

    #[test]
    fn test_literal() -> Result<()> {
        let regex = "A";
        assert_eq!(test(regex, vec![], true)?, b"A");
        Ok(())
    }

    #[test]
    fn test_char_repetition_exact() -> Result<()> {
        let regex = "A{2}";
        assert_eq!(test(regex, vec![], true)?, b"AA");
        Ok(())
    }

    #[test]
    fn test_char_repetition_range_min() -> Result<()> {
        let regex = "A{3,6}";
        assert_eq!(test(regex, vec![0b00000000], true)?, b"AAA");
        Ok(())
    }

    #[test]
    fn test_char_repetition_range_max() -> Result<()> {
        let regex = "A{3,6}";
        assert_eq!(test(regex, vec![0b11111111], true)?, b"AAAAAA");
        Ok(())
    }

    #[test]
    fn test_class_one() -> Result<()> {
        let regex = "[02468]";
        assert_eq!(test(regex, vec![0b00000000], true)?, b"0");
        assert_eq!(test(regex, vec![0b00000001], true)?, b"2");
        assert_eq!(test(regex, vec![0b00000010], true)?, b"4");
        assert_eq!(test(regex, vec![0b00000011], true)?, b"6");
        assert_eq!(test(regex, vec![0b00000100], true)?, b"8");
        Ok(())
    }

    #[test]
    fn test_class_range_one() -> Result<()> {
        let regex = "[0-9]";
        let mut seen = HashSet::new();
        for i in 0..32u8 {
            // Print i as binary
            let r = test(regex, vec![i, i + 1], true)?;
            if let Ok(s) = String::from_utf8(r) {
                seen.insert(s);
            }
        }
        assert!(
            seen.len() == 10,
            "Expected 10 unique digits, got {:?}",
            seen
        );
        Ok(())
    }

    #[test]
    fn test_class_negate_one() -> Result<()> {
        let regex = "[^0]";
        for i in 0..(255 - 9) {
            // Print i as binary
            let r = test(regex, (i..(i + 8)).collect(), true)?;
            if let Ok(s) = String::from_utf8(r) {
                assert_ne!(s, "0");
            }
        }
        Ok(())
    }

    #[test]
    fn test_class_negate_range_one() -> Result<()> {
        let regex = "[^0-9]";
        for i in 0..(255 - 9) {
            // Print i as binary
            let r = test(regex, (i..(i + 8)).collect(), true)?;
            if let Ok(s) = String::from_utf8(r) {
                assert!(s.chars().all(|c| !c.is_ascii_digit()));
            }
        }
        Ok(())
    }

    #[test]
    fn test_concat() -> Result<()> {
        let regex = "A+B+";
        assert_eq!(
            test(regex, vec![0xde, 0xad, 0xbe, 0xef], true)?,
            b"AAAAAABBBB"
        );
        Ok(())
    }

    const JSON_REGEXES: &[&str] = &[
        r#"\\"#,
        r#"(\"|\\|\/|b|f|n|r|t|u)"#,
        r#"[\da-fA-F]"#,
        r#"[0-1]+"#,
        r#"[0-7]+"#,
        r#"[1-9]"#,
        r#".*"#,
        r#"[^*]*\*+([^/*][^*]*\*+)*"#,
    ];

    #[test]
    fn test_json() -> Result<()> {
        let mut rng = thread_rng();
        for regex in JSON_REGEXES {
            println!("Testing regex: {}", regex);
            test(regex, (0..64).map(|_| rng.gen()).collect(), true)?;
        }
        Ok(())
    }
}
