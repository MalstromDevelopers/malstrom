fn main() {
    let p = Some(42);
    let x: Logic<i32> = |y| {
        if p? > y {
            FooBar::Bar
        } else {
            FooBar::Foo
        }
    };
}

pub enum FooBar {
    Foo,
    Bar,
}
impl From<Option<()>> for FooBar {
    fn from(value: Option<()>) -> Self {
        FooBar::Foo
    }
}

pub trait Logic<T>: FnMut(T) -> FooBar + 'static {}
impl<T, F: FnMut(T) -> FooBar + 'static> Logic<T> for F {}
