use std::time::Duration;

use dipper_pgmq::{JobBuilder, JobGuard, PgQueue};
use fake::{Dummy, Fake, Faker};
use pgtemp::PgTempDB;
use sqlx::{Pool, Postgres};

/// Initialize a temporary database for integration testing.
///
/// This function creates a temporary database and runs the migrations.
/// It returns the database connection pool and the temporary database guard.
async fn temp_pgmq_db() -> (Pool<Postgres>, PgTempDB) {
    let temp_db = PgTempDB::new();
    let db = Pool::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to temporary database");
    dipper_pgmq::run_db_migrations(&db)
        .await
        .expect("Failed to run DB migrations");
    (db, temp_db)
}

/// A test message for integration testing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestMsg {
    data: String,
}

impl Dummy<Faker> for TestMsg {
    fn dummy_with_rng<R: fake::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
        Self {
            data: Dummy::dummy_with_rng(config, rng),
        }
    }
}

#[tokio::test]
async fn push_and_pop_job() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    let jobs = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn job_pulled_only_once() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    let jobs1: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    let jobs2: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // The job should be pulled only once, the second pull should return no jobs
    assert!(jobs1.is_some());
    assert!(jobs2.is_none());

    // Assert the message is the same as the one we pushed
    let job1: JobGuard<TestMsg> = jobs1.expect("Failed to get job from queue");
    assert_eq!(job1.id(), &job_id);
    assert_eq!(job1.desc().data, msg.data);
}

#[tokio::test]
async fn scheduled_job_becomes_available() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(500));

    //* When
    // Push a message and schedule the job for the future (500 milliseconds from now)
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).schedule_at(msg_schedule))
        .await
        .expect("Failed to push message to queue");

    // Wait for the job to be available
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Pull the jobs from the queue
    let jobs = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled, as it is ready to be pulled
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn scheduled_job_not_available_early() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_add(time::Duration::minutes(1));

    //* When
    // Push a message and schedule the job for the future (1 minute from now)
    let _id = queue
        .push(JobBuilder::new(msg.clone()).schedule_at(msg_schedule))
        .await
        .expect("Failed to push message to queue");

    // Pull the jobs from the queue immediately
    let jobs: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the job is scheduled for the future
    assert!(jobs.is_none());
}

#[tokio::test]
async fn past_scheduled_job_immediately_available() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_sub(time::Duration::minutes(5));

    //* When
    // We push a message and schedule the job in the past (5 minutes ago)
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).schedule_at(msg_schedule))
        .await
        .expect("Failed to push message to queue");

    let jobs = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled, as the message was scheduled for the past
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn clear_queue_removes_all_jobs() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // We push a message for immediate processing
    queue
        .push(msg)
        .await
        .expect("Failed to push message to queue");

    // Clear all jobs from the queue
    queue.clear().await.expect("Failed to clear queue");

    let jobs: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the queue was cleared
    assert!(jobs.is_none());
}

#[tokio::test]
async fn remove_job_after_processing() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let _ = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Get a job from the queue
    let job: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    // Remove the job from the queue, e.g., after successfully executing the job
    let job = job.expect("Failed to get job from queue");
    job.remove().await.expect("Failed to remove job from queue");

    // Pull the message from the queue
    let job: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the job was removed
    assert!(job.is_none());
}

#[tokio::test]
async fn failed_job_gets_rescheduled() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Pull a job from the queue, mark them as "RUNNING"
    let job: Option<JobGuard<TestMsg>> = queue.pop().await.expect("Failed to pull jobs from queue");

    // Mark the job as failed, and reschedule it for immediate execution (`None`)
    let job = job.expect("Failed to get job from queue");
    job.mark_as_failed()
        .await
        .expect("Failed to mark job as failed");

    // Pull the jobs from the queue
    let jobs: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled again, as it was marked as failed
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn failed_job_rescheduled_for_future() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Pull a job from the queue, marking it as "RUNNING"
    let job: Option<JobGuard<TestMsg>> = queue.pop().await.expect("Failed to pull jobs from queue");

    // Mark the job as failed, and reschedule it for the future (500 milliseconds from now)
    let job = job.expect("Failed to get job from queue");
    let msg_schedule =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(500));
    job.mark_as_failed_and_reschedule(msg_schedule)
        .await
        .expect("Failed to mark job as failed");

    // Pull the jobs from the queue
    let jobs1: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    // Wait for the failed (and re-scheduled) job to be available
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Pull the jobs from the queue
    let jobs2: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // The job should not be pulled if the job is rescheduled for the future and pulled immediately
    assert!(jobs1.is_none());

    // Assert the job is pulled again, as it was marked as failed
    assert!(jobs2.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs2.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn custom_max_retries_sets_max_attempts() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();
    let custom_max_retries = 4u32; // 4 retries = 5 total attempts

    //* When
    // Push a job with custom max_retries using JobBuilder
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).max_retries(custom_max_retries))
        .await
        .expect("Failed to push message to queue");

    //* Then
    // Query the database directly to verify max_attempts was set correctly (retries + 1)
    let (max_attempts,): (i32,) =
        sqlx::query_as("SELECT max_attempts FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to query max_attempts from database");
    assert_eq!(
        max_attempts,
        (custom_max_retries + 1) as i32,
        "max_attempts should be retries + 1"
    );

    // Also verify the job can be popped and has the correct data
    let job: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    assert!(job.is_some());
    let job = job.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn default_max_attempts_value() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Push a job without setting max_attempts (should use default)
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    //* Then
    // Query the database directly to verify max_attempts uses the default value (3)
    let (max_attempts,): (i32,) =
        sqlx::query_as("SELECT max_attempts FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to query max_attempts from database");
    assert_eq!(max_attempts, 3, "Default max_attempts should be 3"); // DEFAULT_MAX_ATTEMPTS is 3
}

#[tokio::test]
async fn max_retries_with_scheduled_job() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();
    let custom_max_retries = 6u32; // 6 retries = 7 total attempts
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_add(time::Duration::minutes(1));

    //* When
    // Push a scheduled job with custom max_retries
    let job_id = queue
        .push(
            JobBuilder::new(msg.clone())
                .max_retries(custom_max_retries)
                .schedule_at(msg_schedule),
        )
        .await
        .expect("Failed to push scheduled message to queue");

    //* Then
    // Query the database directly to verify max_attempts was set correctly for scheduled job (retries + 1)
    let row: (i32,) = sqlx::query_as("SELECT max_attempts FROM pgmq_queue WHERE id = $1")
        .bind(job_id)
        .fetch_one(&db)
        .await
        .expect("Failed to query max_attempts from database");

    let actual_max_attempts = row.0;
    assert_eq!(actual_max_attempts, (custom_max_retries + 1) as i32);
}
