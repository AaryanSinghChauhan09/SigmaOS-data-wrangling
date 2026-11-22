//! SigmaOS component binary: data-wrangling

use sigmaos_data-wrangling::Component;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Component::new().operate()?;
    println!("sigmaos-data-wrangling: ok");
    Ok(())
}
