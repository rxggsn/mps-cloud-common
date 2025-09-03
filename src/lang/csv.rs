use std::fmt;

pub struct Csv<'a> {
    pub headers: &'a [String],
    pub rows: Vec<&'a Vec<String>>,
}
impl<'a> Csv<'a> {
    pub fn new(headers: &'a [String]) -> Self {
        Csv {
            headers,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: &'a Vec<String>) -> &mut Self {
        self.rows.push(row);
        self
    }
}

impl<'a> fmt::Display for Csv<'a> {
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
