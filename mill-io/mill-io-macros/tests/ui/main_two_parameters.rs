use mill_io::EventLoop;

#[mill_io::main]
#[allow(dead_code)]
fn serve(first: &EventLoop, second: &EventLoop) {
    let _ = (first, second);
}

fn main() {}
