use std::fmt;

pub struct Csv {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}
impl Csv {
    pub fn new(headers: &[String]) -> Self {
        Csv {
            headers: headers.to_vec(),
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<String>) -> &mut Self {
        self.rows.push(row);
        self
    }
}

impl fmt::Display for Csv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write headers
        writeln!(f, "{}", self.headers.join(","))?;
        // Write each row
        for row in &self.rows {
            writeln!(f, "{}", row.join(","))?;
        }
        Ok(())
    }
}
