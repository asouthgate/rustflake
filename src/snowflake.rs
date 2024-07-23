use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::sync::{Arc, Mutex};
use std::thread;

/// Snowflake ID generation with a small variation
#[derive(Clone)]
pub struct SnowFlakeIdGenerator {
    epoch: u64,
    _machine_id: u64, // allows 255 machines, for now unused
    max_shard_id: u64,
    n_shard_id_bits: u64,
    n_machine_id_bits: u64,
    n_counter_bits: u64,
    n_timestamp_bits: u64,
    shard_id_shift: u64,
    timestamp_shift: u64,
    machine_id_shift: u64,
    machine_id_bits: u64,
    counters: Vec<Arc<Mutex<u64>>>, // this does not use the whole 64 bits, it has a max
    counter_max: u64, // some max value given we only use n_sequence_bits bits
}

impl SnowFlakeIdGenerator {
    pub fn new(
        epoch: u64, 
        machine_id: u64,
        n_machine_id_bits: u64,
        n_shard_id_bits: u64,
        n_counter_bits: u64
    ) -> SnowFlakeIdGenerator {
        let n_timestamp_bits = 41;
        // 63 because postgres doesn't give u64, only i64 :(
        if n_machine_id_bits + n_shard_id_bits + n_counter_bits + n_timestamp_bits != 63 {
            panic!(
                "Invalid number of bits requested for snowflake {} + {} + {} + {}. It must equal 63 for u64.", 
                n_machine_id_bits, n_shard_id_bits, n_counter_bits, n_timestamp_bits
            );
        }
        if machine_id > (1 << n_machine_id_bits) {
            panic!("Machine ID {} greater than the maximum {}", machine_id, n_machine_id_bits);
        }
        let n_shards = 1 << n_shard_id_bits;
        let mut mutexes = vec![];
        for _ in 0..n_shards {
            mutexes.push(Arc::new(Mutex::new(0)));
        }
        SnowFlakeIdGenerator {
            epoch,
            _machine_id: machine_id,
            n_shard_id_bits,
            n_machine_id_bits,
            n_counter_bits,
            n_timestamp_bits,        
            max_shard_id: n_shards - 1,
            shard_id_shift: n_counter_bits,
            machine_id_bits: (machine_id << (n_counter_bits + n_shard_id_bits)),
            machine_id_shift: n_counter_bits + n_shard_id_bits,
            timestamp_shift: n_counter_bits + n_shard_id_bits + n_machine_id_bits,
            counter_max: (1 << n_counter_bits) - 1,
            counters: mutexes,
        }
    }

    pub fn new_with_defaults(machine_id: u64) -> Self {
        // 2021-01-01 @ 00:00:00 UTC
        SnowFlakeIdGenerator::new(1609459200000, machine_id, 5, 5, 12)
    }

    pub fn generate_id(&self, shard_id: u64) -> Result<u64, String> {

        if shard_id > self.max_shard_id {
            return Err(format!("Cannot request shard_id {}, max is {}",
                shard_id,
                self.max_shard_id
            ));
        }

        // If sequence overflowed, wait 1 ms
        let mut count = self.counters[shard_id as usize].lock().unwrap();
        if *count == 0 {
            thread::sleep(Duration::from_millis(1));
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64 - self.epoch;

        let id = (timestamp << self.timestamp_shift)
            | self.machine_id_bits
            | (shard_id << self.shard_id_shift)
            | *count;

        *count = (*count + 1) % self.counter_max;
        Ok(id)
    }

    fn get_bits(&self, start: u64, end: u64, snowflake_id: u64) -> u64 {
        let nbits = end - start;
        let mask = (1 << nbits) - 1; // e.g. nbits=5; 100000 - 1 = 011111
        let shifted_mask = mask << start;
        (snowflake_id & shifted_mask) >> start
    }

    pub fn get_count(&self, snowflake_id: u64) -> u64{ 
        self.get_bits(
            0, 
            self.n_counter_bits, 
            snowflake_id
        )
    }

    pub fn get_shard_id(&self, snowflake_id: u64) -> u64{ 
        self.get_bits(
            self.shard_id_shift, 
            self.shard_id_shift + self.n_shard_id_bits, 
            snowflake_id
        )
    }

    pub fn get_machine_id(&self, snowflake_id: u64) -> u64 {
        self.get_bits(
            self.machine_id_shift, 
            self.machine_id_shift + self.n_machine_id_bits, 
            snowflake_id
        )
    }

    pub fn get_timestamp(&self, snowflake_id: u64) -> u64 {
        self.get_bits(
            self.timestamp_shift, 
            self.timestamp_shift + self.n_timestamp_bits,
            snowflake_id
        )
    }

    pub fn get_datetime(&self, snowflake_id: u64) -> DateTime::<Utc> {
        let ms = self.get_bits(
            self.timestamp_shift, 
            self.timestamp_shift + self.n_timestamp_bits,
            snowflake_id
        ) + self.epoch;
        let naive_date_time = NaiveDateTime::from_timestamp_millis(ms as i64).unwrap();
        DateTime::<Utc>::from_naive_utc_and_offset(naive_date_time, Utc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_machine_and_shard_ids_consistent() {
        // Test that what you put in is what you get out
        for machine_id in 0..32 {
            let sfg = SnowFlakeIdGenerator::new_with_defaults(machine_id);    
            for shard_id in 0..32 {
                let id = sfg.generate_id(shard_id).unwrap();
                let retrieved_machine_id = sfg.get_machine_id(id);
                assert!(retrieved_machine_id == machine_id);       
                let retrieved_shard_id = sfg.get_shard_id(id);
                assert!(retrieved_shard_id == shard_id);         
            }
        }
    }

    #[test]
    fn test_counter_monotonic() {
        let sfg = SnowFlakeIdGenerator::new_with_defaults(0);    
        for counter in 0..sfg.counter_max {
            let id = sfg.generate_id(0).unwrap();
            assert!(sfg.get_count(id) == counter);
        }
    }

    #[test]
    fn test_id_dates() {
        let sfg = SnowFlakeIdGenerator::new_with_defaults(0);    
        let start = Utc::now();
        let mut dates = vec![];
        let n_ids: usize = sfg.counter_max as usize * 100;
        for _ in 0..n_ids { // too big for one millisecond
            let id = sfg.generate_id(0).unwrap();
            let date = sfg.get_datetime(id);
            dates.push(date);
            assert!(date >= start);
        }
        assert!(dates[0] < dates[n_ids - 1]); // strictly less than, since ids should have overflowed & slept
        for di in 1..n_ids {
            assert!(dates[di - 1] <= dates[di]);
        }
    }

    #[test]
    fn test_small_uniqueness() {
        let sfg = SnowFlakeIdGenerator::new_with_defaults(0);    
        let mut ids = vec![];
        let n_ids = 1000000;
        for _counter in 0..n_ids {
            let id = sfg.generate_id(0).unwrap();
            ids.push(id);
        }
        let unique_ids: std::collections::HashSet<_> = ids.into_iter().collect();
        assert!(unique_ids.len() == n_ids);
    
    }

}
