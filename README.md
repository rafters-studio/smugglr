# smugglr

A SQLite sync engine. Greenfield rebuild in progress.

This tree is intentionally empty. The rebuild starts from service design and
specification, not from the prior code.

The previous implementation is preserved two ways:

- a full working copy at `../smugglr-archive`
- this repository's own git history, through commit `1ff0b75`

Every change smugglr carries between nodes has a sunset, the graveyard
setting (12 hours by default), and past it the change is dead. Clocks should
agree to within one twelfth of the sunset.

Nothing here is built yet. Code arrives when the spec calls for it. Until
then, `smugglr` prints its name and version (`smugglr 0.1.0`) and exits.
