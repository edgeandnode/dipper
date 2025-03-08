// Set the `cargo:rustc-link-search` variable to the path of the Python library.
//
//  1. If the `PYTHON_LIB_DIR` environment variable is set, use it.
//  2. Otherwise, use the `uv tool run find_libpython` command to find the path.
//
// If neither of these options work, do not set the variable.
fn main() {
    // Instruct cargo to rerun this build script if the `../../migrations` directory changes
    println!("cargo:rerun-if-changed=../../migrations");

    // Check if the `PYTHON_LIB_DIR` environment variable is set
    let python_lib_dir = if let Ok(path) = std::env::var("PYTHON_LIB_DIR") {
        // Instruct cargo to rerun this build script if the `PYTHON_LIB_DIR` env variable change.
        println!("cargo:rerun-if-env-changed=PYTHON_LIB_DIR");

        Some(path)
    } else {
        // Run the `uv tool run find_libpython` command to find the path to the Python library.
        // See: https://pypi.org/project/find-libpython/
        let output = std::process::Command::new("uv")
            .arg("tool")
            .arg("run")
            .arg("find_libpython")
            .output();

        match output {
            Ok(output) if output.status.success() => {
                // Parse the output of the `uv tool run find_libpython` command into a path.
                let path = String::from_utf8(output.stdout).expect("invalid UTF-8 output from uv");

                // Get the parent directory of the Python library path.
                let path = std::path::PathBuf::from(path.trim())
                    .parent()
                    .expect("libpython path has no parent directory")
                    .to_str()
                    .expect("libpython path is not valid UTF-8")
                    .to_string();
                Some(path)
            }
            _ => {
                println!(
                    "cargo:warning=Failed to determine libpython path using `uv tool run find_libpython`"
                );
                None
            }
        }
    };

    if let Some(python_lib_dir) = python_lib_dir {
        // Instruct the linker to search for the Python library in the specified path.
        println!("cargo:rustc-link-search={}", python_lib_dir);

        // Set the `LD_LIBRARY_PATH` env variable for the `cargo run` and `cargo test` commands.
        // See: https://stackoverflow.com/questions/51796417
        println!("cargo:rustc-env=LD_LIBRARY_PATH={}", python_lib_dir);
    }
}
