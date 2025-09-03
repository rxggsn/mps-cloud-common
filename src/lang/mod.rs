pub mod graph;
pub mod markdown;
pub mod slice;
pub mod vec;
pub mod csv;

#[macro_export]
macro_rules! impl_enum {
    ($enum:ident,$value:expr,$name:expr) => {
        pub struct $enum;

        impl $enum {
            pub fn value() -> u8 {
                $value
            }

            pub fn name() -> &'static str {
                $name
            }
        }
    };
}

#[macro_export]
macro_rules! extract_enum_desc {
    ($name:ident,$($enum:ident),*) => {
        pub fn $name(val: u8) -> &'static str{
            $(
                if val == $enum::value() {
                    return $enum::name();
                }
            )* else {
                return "Unknown";
            }
        }
    };
}
