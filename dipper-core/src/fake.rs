/// # uuid
///
/// Please note that :
/// *  all [`Dummy`] implementations for [`String`] use [to_hyphenated](https://docs.rs/uuid/latest/uuid/struct.Uuid.html#method.to_hyphenated).
/// *  [`Dummy<Faker>`] implementation uses [from_u128](https://docs.rs/uuid/latest/uuid/struct.Uuid.html#method.from_u128)
pub mod uuid {
    use fake::{Fake, Faker, Rng};

    /// As per [new_v7](https://docs.rs/uuid/latest/uuid/struct.Uuid.html#method.new_v7).
    pub struct UUIDv7;

    impl fake::Dummy<UUIDv7> for uuid::Uuid {
        fn dummy_with_rng<R: Rng + ?Sized>(_: &UUIDv7, rng: &mut R) -> Self {
            let ticks = rng.gen_range(uuid::timestamp::UUID_TICKS_BETWEEN_EPOCHS..u64::MAX);
            let counter = Faker.fake_with_rng(rng);
            let ts = uuid::timestamp::Timestamp::from_gregorian(ticks, counter);
            uuid::Uuid::new_v7(ts)
        }
    }

    impl fake::Dummy<UUIDv7> for String {
        fn dummy_with_rng<R: fake::Rng + ?Sized>(config: &UUIDv7, rng: &mut R) -> Self {
            uuid::Uuid::dummy_with_rng(config, rng)
                .hyphenated()
                .to_string()
        }
    }
}
