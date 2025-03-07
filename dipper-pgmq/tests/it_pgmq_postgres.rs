use std::time::Duration;

use dipper_pgmq::{Job, Queue, postgres::PgQueue};
use fake::{Dummy, Fake, Faker};
use sqlx::{Pool, Postgres};

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

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled
    assert_eq!(jobs.len(), 1);

    // Assert the message is the same as the one we pushed
    assert_eq!(jobs[0].id, job_id);
    assert_eq!(jobs[0].message.data, msg.data);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_pull_multiple_times(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    let jobs1: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    let jobs2: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // The job should be pulled only once, the second pull should return no jobs
    assert_eq!(jobs1.len(), 1);
    assert_eq!(jobs2.len(), 0);

    // Assert the message is the same as the one we pushed
    assert_eq!(jobs1[0].id, job_id);
    assert_eq!(jobs1[0].message.data, msg.data);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_scheduled(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
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
    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled, as it is ready to be pulled
    assert_eq!(jobs.len(), 1);

    // Assert the message is the same as the one we pushed
    assert_eq!(jobs[0].id, job_id);
    assert_eq!(jobs[0].message.data, msg.data);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_scheduled_pull_too_early(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
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
    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the job is scheduled for the future
    assert_eq!(jobs.len(), 0);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_scheduled_past(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_sub(time::Duration::minutes(5));

    //* When
    // We push a message and schedule the job in the past (5 minutes ago)
    let job_id = queue
        .push_scheduled(msg.clone(), msg_schedule)
        .await
        .expect("Failed to push message to queue");

    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled, as the message was scheduled for the past
    assert_eq!(jobs.len(), 1);

    // Assert the message is the same as the one we pushed
    assert_eq!(jobs[0].id, job_id);
    assert_eq!(jobs[0].message.data, msg.data);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_and_clear_queue(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // We push a message for immediate processing
    queue
        .push(msg)
        .await
        .expect("Failed to push message to queue");

    // Clear all jobs from the queue
    <PgQueue as Queue<TestMsg>>::clear(&queue)
        .await
        .expect("Failed to clear queue");

    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the queue was cleared
    assert_eq!(jobs.len(), 0);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_and_remove(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Remove the job from the queue, e.g., after successfully executing the job
    <PgQueue as Queue<TestMsg>>::remove(&queue, job_id)
        .await
        .expect("Failed to remove job from queue");

    // Pull the message from the queue
    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Ass
    assert_eq!(jobs.len(), 0);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_mark_as_failed(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Pull a job from the queue, mark them as "RUNNING"
    let _: Vec<Job<TestMsg>> = queue.pull(5).await.expect("Failed to pull jobs from queue");

    // Mark the job as failed, and reschedule it for immediate execution (`None`)
    <PgQueue as Queue<TestMsg>>::mark_job_as_failed(&queue, job_id, None)
        .await
        .expect("Failed to remove job from queue");

    // Pull the jobs from the queue
    let jobs: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled again, as it was marked as failed
    assert_eq!(jobs.len(), 1);

    // Assert the message is the same as the one we pushed
    assert_eq!(jobs[0].id, job_id);
    assert_eq!(jobs[0].message.data, msg.data);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn push_job_mark_as_failed_and_reschedule(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Pull a job from the queue, marking it as "RUNNING"
    let _: Vec<Job<TestMsg>> = queue.pull(5).await.expect("Failed to pull jobs from queue");

    // Mark the job as failed, and reschedule it for the future (500 milliseconds from now)
    let msg_schedule =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(500));
    <PgQueue as Queue<TestMsg>>::mark_job_as_failed(&queue, job_id, Some(msg_schedule))
        .await
        .expect("Failed to remove job from queue");

    // Pull the jobs from the queue
    let jobs1: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    // Wait for the failed (and re-scheduled) job to be available
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Pull the jobs from the queue
    let jobs2: Vec<Job<TestMsg>> = queue
        .pull(5)
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // The job should not be pulled if the job is rescheduled for the future and pulled immediately
    assert_eq!(jobs1.len(), 0);

    // Assert the job is pulled again, as it was marked as failed
    assert_eq!(jobs2.len(), 1);

    // Assert the message is the same as the one we pushed
    assert_eq!(jobs2[0].id, job_id);
    assert_eq!(jobs2[0].message.data, msg.data);

    Ok(())
}
