use clap::Parser;
use futures::{Stream, lock::Mutex, StreamExt};
use lighthouse_client::{Lighthouse, Result, TokioWebSocket, LIGHTHOUSE_URL, protocol::{Authentication, Color, Delta, Frame, Pos, ServerMessage, LIGHTHOUSE_RECT, LIGHTHOUSE_SIZE}};
use lighthouse_protocol::Model;
use tracing::{info, debug};
use tokio::{task, time};
use std::{collections::{VecDeque, HashSet}, sync::Arc, time::Duration};

const UPDATE_INTERVAL: Duration = Duration::from_millis(200);
const FRUIT_COLOR: Color = Color::RED;
const SNAKE_COLOR: Color = Color::GREEN;
const SNAKE_INITIAL_LENGTH: usize = 3;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Snake {
    fields: VecDeque<Pos>,
    dir: Delta,
}

impl Snake {
    fn from_initial_length(length: usize) -> Self {
        let mut pos: Pos = LIGHTHOUSE_RECT.sample_random().unwrap();
        let dir = Delta::random_cardinal();

        let mut fields = VecDeque::new();
        for _ in 0..length {
            fields.push_back(pos);
            pos = LIGHTHOUSE_RECT.wrap(pos - dir);
        }

        Self { fields, dir }
    }

    fn head(&self) -> Pos { *self.fields.front().unwrap() }

    fn back(&self) -> Pos { *self.fields.back().unwrap() }

    fn grow(&mut self) {
        self.fields.push_back(LIGHTHOUSE_RECT.wrap(self.back() - self.dir));
    }

    fn step(&mut self) {
        let head = self.head();
        self.fields.pop_back();
        self.fields.push_front(LIGHTHOUSE_RECT.wrap(head + self.dir));
    }

    fn intersects_itself(&self) -> bool {
        self.fields.iter().collect::<HashSet<_>>().len() < self.fields.len()
    }

    fn rotate_head(&mut self, dir: Delta) {
        self.dir = dir;
    }

    fn render_to(&self, frame: &mut Frame) {
        for field in &self.fields {
            frame[*field] = SNAKE_COLOR;
        }
    }

    fn len(&self) -> usize {
        self.fields.len()
    }

    fn random_fruit_pos(&self) -> Option<Pos> {
        let fields = self.fields.iter().collect::<HashSet<_>>();
        if fields.len() >= LIGHTHOUSE_SIZE {
            None
        } else {
            loop {
                let pos = LIGHTHOUSE_RECT.sample_random().unwrap();
                if !fields.contains(&pos) {
                    break Some(pos);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct State {
    snake: Snake,
    fruit: Pos,
}

impl State {
    fn new() -> Self {
        let snake = Snake::from_initial_length(SNAKE_INITIAL_LENGTH);
        let fruit = snake.random_fruit_pos().unwrap();
        Self { snake, fruit }
    }
    
    fn reset(&mut self) {
        *self = Self::new();
    }

    fn step(&mut self) {
        self.snake.step();

        if self.snake.head() == self.fruit {
            self.snake.grow();
            let length = self.snake.len();
            info! { %length, "Snake grew" };
            if let Some(fruit) = self.snake.random_fruit_pos() {
                self.fruit = fruit;
            } else {
                info!("You win!");
                self.reset();
            }
        } else if self.snake.intersects_itself() {
            info!("Game over!");
            self.reset();
        }
    }

    fn render(&self) -> Frame {
        let mut frame = Frame::empty();

        frame[self.fruit] = FRUIT_COLOR;
        self.snake.render_to(&mut frame);

        frame
    }
}

async fn run_updater(lh: Lighthouse<TokioWebSocket>, shared_state: Arc<Mutex<State>>) -> Result<()> {
    loop {
        // Update the snake and render it
        let frame = {
            let mut state = shared_state.lock().await;
            state.step();
            state.render()
        };

        // Send the rendered snake to the lighthouse
        lh.put_model(frame).await?;
        debug!("Sent frame");

        // Wait for a short period of time
        time::sleep(UPDATE_INTERVAL).await;
    }
}

async fn run_controller(mut stream: impl Stream<Item = Result<ServerMessage<Model>>> + Unpin, shared_state: Arc<Mutex<State>>) -> Result<()> {
    while let Some(msg) = stream.next().await {
        if let Model::InputEvent(event) = msg?.payload {
            if event.is_down {
                // Map the key code to a direction vector
                let opt_dir = match event.key {
                    Some(37) => Some(Delta::LEFT),
                    Some(38) => Some(Delta::UP),
                    Some(39) => Some(Delta::RIGHT),
                    Some(40) => Some(Delta::DOWN),
                    _ => None,
                };

                // Update the snake's direction
                if let Some(dir) = opt_dir {
                    debug!("Rotating snake head to {:?}", dir);
                    let mut state = shared_state.lock().await;
                    state.snake.rotate_head(dir);
                }
            }
        }
    }

    Ok(())
}

#[derive(Parser)]
struct Args {
    /// The username.
    #[arg(short, long, env = "LIGHTHOUSE_USER")]
    username: String,
    /// The API token.
    #[arg(short, long, env = "LIGHTHOUSE_TOKEN")]
    token: String,
    /// The server URL.
    #[arg(long, env = "LIGHTHOUSE_URL", default_value = LIGHTHOUSE_URL)]
    url: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    _ = dotenvy::dotenv();

    let args = Args::parse();
    let auth = Authentication::new(&args.username, &args.token);
    let state = Arc::new(Mutex::new(State::new()));

    let lh = Lighthouse::connect_with_tokio_to(&args.url, auth).await?;
    info!("Connected to the Lighthouse server");

    let stream = lh.stream_model().await?;

    let updater_handle = task::spawn(run_updater(lh, state.clone()));
    let controller_handle = task::spawn(run_controller(stream, state));

    updater_handle.await.unwrap()?;
    controller_handle.await.unwrap()?;

    Ok(())
}
