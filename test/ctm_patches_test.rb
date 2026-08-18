# vim:fileencoding=utf-8
require_relative 'test_helper'

# Covers the patches this fork carries on top of upstream, so a rebase onto a
# future upstream release fails loudly rather than dropping one of them.
context 'CTM patches' do
  setup do
    Resque::Scheduler.configure do |c|
      c.dynamic = false
      c.quiet = true
      c.env = nil
      c.app_name = nil
    end
    Resque.data_store.redis.flushall
    Resque::Scheduler.clear_schedule!
    Resque::Scheduler.send(:instance_variable_set, :@scheduled_jobs, {})
    Resque::Scheduler.send(:instance_variable_set, :@shutdown, false)
    Resque::Scheduler.delayed_lockless = false
  end

  # am_master memoizes, and the schedule tests start a rufus scheduler, so both
  # have to be put back or the rest of the suite inherits them.
  teardown do
    Resque::Scheduler.delayed_lockless = false
    if Resque::Scheduler.instance_variable_defined?(:@am_master)
      Resque::Scheduler.send(:remove_instance_variable, :@am_master)
    end
    Resque::Scheduler.clear_schedule!
    Resque.schedule = {}
    Object.send(:remove_const, :StatsTracker) if Object.const_defined?(:StatsTracker)
  end

  # Two jobs sitting in one delayed timestamp, with this process holding no
  # master lock. The timestamp is in the future because enqueue_at queues a
  # past one immediately instead of delaying it.
  def delayed_items_without_the_lock
    timestamp = Time.now + 60
    2.times { Resque.enqueue_at(timestamp, SomeIvarJob) }
    Resque::Scheduler.send(:instance_variable_set, :@am_master, false)
    timestamp
  end

  def bad_and_good_schedule
    {
      'bad_entry' => { 'cron' => 'not a cron expression', 'class' => 'SomeIvarJob' },
      'good_entry' => { 'cron' => '* * * * *', 'class' => 'SomeIvarJob' }
    }
  end

  def delayed_page_helper
    Object.new.extend(Resque::Scheduler::Server::HelperMethods)
  end

  # Runs the given block the next time anything WATCHes, i.e. in the window
  # between a batch being read and its transaction committing. Standing in for a
  # second run_delayed_only process lets the race be exercised in one thread,
  # with no sleeps and no ordering luck.
  module RaceHook
    class << self
      attr_accessor :on_next_watch
    end

    def watch(*args, &block)
      interloper = RaceHook.on_next_watch
      RaceHook.on_next_watch = nil
      interloper&.call
      super
    end
  end

  def before_the_next_watch(&block)
    Resque.redis.redis.singleton_class.prepend(RaceHook)
    RaceHook.on_next_watch = block
  end

  test 'the delayed loop enqueues without the master lock when lockless' do
    timestamp = delayed_items_without_the_lock
    Resque::Scheduler.delayed_lockless = true

    Resque::Scheduler.enqueue_delayed_items_for_timestamp(timestamp)

    assert_equal(2, Resque.size(Resque.queue_from_class(SomeIvarJob)))
  end

  test 'the delayed loop drains the timestamp when lockless' do
    timestamp = delayed_items_without_the_lock
    Resque::Scheduler.delayed_lockless = true

    Resque::Scheduler.enqueue_delayed_items_for_timestamp(timestamp)

    assert_equal(0, Resque.delayed_timestamp_size(timestamp))
  end

  test 'the delayed loop enqueues nothing when neither lockless nor master' do
    timestamp = delayed_items_without_the_lock

    Resque::Scheduler.enqueue_delayed_items_for_timestamp(timestamp)

    assert_equal(0, Resque.size(Resque.queue_from_class(SomeIvarJob)))
  end

  test 'the delayed loop leaves the timestamp alone when neither lockless nor master' do
    timestamp = delayed_items_without_the_lock

    Resque::Scheduler.enqueue_delayed_items_for_timestamp(timestamp)

    assert_equal(2, Resque.delayed_timestamp_size(timestamp))
  end

  test 'a lockless batch does not requeue jobs another process already drained' do
    Resque::Scheduler.delayed_lockless = true
    timestamp = Time.now + 60
    Resque.enqueue_at(timestamp, SomeIvarJob, 'a')
    Resque.enqueue_at(timestamp, SomeIvarJob, 'b')
    queue = Resque.queue_from_class(SomeIvarJob)

    before_the_next_watch do
      # What a second delayed process does: enqueue both jobs and empty the bucket.
      Resque.delayed_timestamp_peek(timestamp, 0, 2).each do |job|
        Resque::Job.create(queue, job['class'], *job['args'])
      end
      Resque.redis.ltrim("delayed:#{timestamp.to_i}", 2, -1)
    end

    Resque::Scheduler.enqueue_items_in_batch_for_timestamp(timestamp, 100)

    assert_equal(2, Resque.size(queue))
  end

  test 'a lockless batch does not discard a job that arrived while it worked' do
    Resque::Scheduler.delayed_lockless = true
    timestamp = Time.now + 60
    Resque.enqueue_at(timestamp, SomeIvarJob, 'a')
    queue = Resque.queue_from_class(SomeIvarJob)
    key = "delayed:#{timestamp.to_i}"

    before_the_next_watch do
      # A second delayed process takes 'a', and a fresh 'c' lands in the bucket
      # behind it. An LTRIM against the stale read would drop 'c' unqueued.
      Resque::Job.create(queue, 'SomeIvarJob', 'a')
      Resque.redis.ltrim(key, 1, -1)
      Resque.enqueue_at(timestamp, SomeIvarJob, 'c')
    end

    Resque::Scheduler.enqueue_items_in_batch_for_timestamp(timestamp, 100)

    assert(Resque.peek(queue, 0, 10).map { |job| job['args'] }.include?(['c']),
           'the job that arrived mid-batch was dropped without being queued')
  end

  test 'a schedule rufus cannot parse does not take down the load' do
    Resque.schedule = bad_and_good_schedule

    Resque::Scheduler.load_schedule!
  end

  test 'a schedule rufus cannot parse does not get scheduled' do
    Resque.schedule = bad_and_good_schedule
    Resque::Scheduler.load_schedule!

    assert_nil(Resque::Scheduler.scheduled_jobs['bad_entry'])
  end

  test 'a schedule rufus cannot parse does not stop the entries around it' do
    Resque.schedule = bad_and_good_schedule
    Resque::Scheduler.load_schedule!

    assert(Resque::Scheduler.scheduled_jobs['good_entry'])
  end

  test 'the delayed page names a queue without loading the job class' do
    assert_nil(delayed_page_helper.queue_from_class_name('AJobClassThisProcessCannotLoad'))
  end

  test 'the delayed page still resolves the queue for a class it can load' do
    assert_equal(Resque.queue_from_class(SomeIvarJob).to_s,
                 delayed_page_helper.queue_from_class_name('SomeIvarJob').to_s)
  end

  test 'a fired schedule reports its job class to StatsTracker' do
    tracker = mock('StatsTracker')
    tracker.expects(:increment).with(
      'ResqueScheuler.enqueue', tags: ['class_name:SomeIvarJob']
    )
    Object.const_set(:StatsTracker, tracker)
    Resque::Scheduler.send(:instance_variable_set, :@am_master, true)

    Resque::Scheduler.send(:enqueue_recurring, 'some_job',
                           'cron' => '* * * * *', 'class' => 'SomeIvarJob')
  end

  test 'a fired schedule is queued even when the tracker raises' do
    tracker = mock('StatsTracker')
    tracker.stubs(:increment).raises(StandardError, 'statsd is down')
    Object.const_set(:StatsTracker, tracker)
    Resque::Scheduler.send(:instance_variable_set, :@am_master, true)
    config = { 'cron' => '* * * * *', 'class' => 'SomeIvarJob' }
    Resque::Scheduler.expects(:enqueue).with(config)

    Resque::Scheduler.send(:enqueue_recurring, 'some_job', config)
  end

  test 'a fired schedule is queued when the app has no tracker' do
    Resque::Scheduler.send(:instance_variable_set, :@am_master, true)
    config = { 'cron' => '* * * * *', 'class' => 'SomeIvarJob' }
    Resque::Scheduler.expects(:enqueue).with(config)

    Resque::Scheduler.send(:enqueue_recurring, 'some_job', config)
  end

  test 'resetting the delayed queue unlinks rather than blocking on del' do
    Resque.enqueue_at(Time.now + 60, SomeIvarJob)
    Resque.redis.expects(:unlink).at_least_once
    Resque.redis.expects(:del).never

    Resque.reset_delayed_queue
  end
end
