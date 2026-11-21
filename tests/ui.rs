//! Finding d3e282fb6f09: the embedded UI renders API values as text.
//! `tests/ui/escape.js` runs the page's own script against hostile API
//! answers (see there); this runs it under node.

use std::process::Command;

#[test]
fn the_ui_renders_hostile_api_values_as_text() {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/ui/escape.js");
    let out = Command::new("node")
        .arg(script)
        .output()
        .expect("node runs tests/ui/escape.js (the UI's output-encoding test)");
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "tests/ui/escape.js failed:\n{stdout}{stderr}");
    assert!(stdout.contains("ok: every API value rendered as text"), "{stdout}{stderr}");
}
