/// Trait for converting a reference to a state into a value.
///
/// This trait is similar to axum's `FromRef` trait.
pub trait FromState<S> {
    fn from_state(state: &S) -> Self;
}

impl<S> FromState<S> for S
where
    S: Clone,
{
    fn from_state(state: &S) -> Self {
        state.clone()
    }
}
