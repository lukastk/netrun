use utcnow::utcnow;

pub fn get_utc_now() -> i128 {
    utcnow().unwrap().as_millis()
}
