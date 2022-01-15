use serde_json::Value;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn transform(arg: &[u8], dat: &[u8]) -> Vec<u8> {
    let v: Value = serde_json::from_str(std::str::from_utf8(arg).unwrap()).unwrap();
    let search = v["search"].as_str().unwrap();
    let replacement = v["replacement"].as_str().unwrap();
    let dat = std::str::from_utf8(dat).unwrap();
    dat.replace(search, replacement).into()
}
