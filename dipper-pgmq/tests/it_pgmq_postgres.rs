use std::time::Duration;

use dipper_pgmq::{JobGuard, PgQueue};
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
async fn push_job() {
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
    assert_eq!(job.message().data, msg.data);
}

#[tokio::test]
async fn push_job_pull_multiple_times() {
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
    assert_eq!(job1.message().data, msg.data);
}

#[tokio::test]
async fn push_job_scheduled() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(500));

    //* When
    // Push a message and schedule the job for the future (500 milliseconds from now)
    let job_id = queue
        .push_scheduled(msg.clone(), msg_schedule)
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
    assert_eq!(job.message().data, msg.data);
}

#[tokio::test]
async fn push_job_scheduled_pull_too_early() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_add(time::Duration::minutes(1));

    //* When
    // Push a message and schedule the job for the future (1 minute from now)
    let _id = queue
        .push_scheduled(msg.clone(), msg_schedule)
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
async fn push_job_scheduled_past() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_sub(time::Duration::minutes(5));

    //* When
    // We push a message and schedule the job in the past (5 minutes ago)
    let job_id = queue
        .push_scheduled(msg.clone(), msg_schedule)
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
    assert_eq!(job.message().data, msg.data);
}

#[tokio::test]
async fn push_job_and_clear_queue() {
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
async fn push_pop_and_remove() {
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
async fn push_pop_mark_as_failed() {
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
    assert_eq!(job.message().data, msg.data);
}

#[tokio::test]
async fn push_job_mark_as_failed_and_reschedule() {
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
    assert_eq!(job.message().data, msg.data);
}
