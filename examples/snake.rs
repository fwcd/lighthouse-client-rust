use async_std::{task, sync::Mutex, channel};
use futures::StreamExt;
use lighthouse_client::{Connection, Authentication, LighthouseResult, LIGHTHOUSE_COLS, LIGHTHOUSE_ROWS, Display, BLACK, LIGHTHOUSE_SIZE, GREEN};
use log::{info, Level};
use rand::prelude::*;
use std::{env, collections::VecDeque, sync::Arc, time::Duration};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Vec2 {
    x: i32,
    y: i32,
}

impl Vec2 {
    fn new(x: i32, y: i32) -> Self {
        Self { x, y }
    }

    fn random_pos() -> Self {
        let mut rng = thread_rng();
        Vec2::new(rng.gen_range(0..(LIGHTHOUSE_COLS as i32)), rng.gen_range(0..(LIGHTHOUSE_ROWS as i32)))
    }

    fn random_dir() -> Self {
        let random_offset = || { if thread_rng().gen() { 1 } else { -1 } };
        if thread_rng().gen() {
            Vec2::new(0, random_offset())
        } else {
            Vec2::new(random_offset(), 0)
        }
    }

    fn pixel_index(self) -> usize {
        self.y as usize * LIGHTHOUSE_COLS + self.x as usize
    }

    fn add_wrapping(self, rhs: Self) -> Self {
        Self::new(
            (self.x + rhs.x).rem_euclid(LIGHTHOUSE_COLS as i32),
            (self.y + rhs.y).rem_euclid(LIGHTHOUSE_ROWS as i32),
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Snake {
    fields: VecDeque<Vec2>,
    dir: Vec2,
}

impl Snake {
    fn new() -> Self {
        let mut fields = VecDeque::new();
        fields.push_back(Vec2::random_pos());
        Self { fields, dir: Vec2::random_dir() }
    }

    fn step(&mut self) {
        let head = *self.fields.front().unwrap();
        self.fields.pop_back();
        self.fields.push_front(head.add_wrapping(self.dir));
    }

    fn render(&self) -> Display {
        let mut pixels = [BLACK; LIGHTHOUSE_SIZE];

        for field in &self.fields {
            pixels[field.pixel_index()] = GREEN;
        }

        Display::new(pixels)
    }
}

async fn run(auth: Authentication) -> LighthouseResult<()> {
    // Set up shared state and a channel to transmit the display
    let shared_snake = Arc::new(Mutex::new(Snake::new()));
    let cloned_snake = shared_snake.clone();
    let (tx, rx) = channel::bounded(1);

    // Launch a task that periodically updates the snake.
    task::spawn(async move {
        loop {
            let display = {
                let mut snake = cloned_snake.lock().await;
                snake.step();
                snake.render()
            };
            tx.send(display).await;
            task::sleep(Duration::from_secs(1)).await;
        }
    });

    // Connect to the lighthouse
    let conn = Connection::new(auth).await?;
    info!("Connected to the Lighthouse server");

    // Request input events
    conn.request_stream().await?;

    // Run the event handler
    loop {
        match rx.next().race(conn.receive_input_event()) {

        }

        task::sleep(Duration::from_secs(1)).await;
    }
}

fn main() {
    simple_logger::init_with_level(Level::Info).unwrap();

    let username = env::var("LIGHTHOUSE_USERNAME").unwrap();
    let token = env::var("LIGHTHOUSE_TOKEN").unwrap();
    let auth = Authentication::new(username.as_str(), token.as_str());

    task::block_on(run(auth)).unwrap();
}
