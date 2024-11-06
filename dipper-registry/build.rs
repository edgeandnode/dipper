//! This build script is used to tell cargo to recompile the crate if the migrations folder changes.
//! https://docs.rs/sqlx/latest/sqlx/macro.migrate.html#triggering-recompilation-on-migration-changes
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
