# vim:fileencoding=utf-8

module Resque
  module Scheduler
    # The entry points this fork adds so the schedule and the delayed queue can
    # run as separate processes. They live here rather than in scheduler.rb so
    # that file stays as close to upstream as possible and rebasing onto a new
    # release stays cheap.
    module CtmRunners
      # Set by run_delayed_only. See #run_delayed_only for why the delayed
      # loop can drop the master lock.
      attr_writer :delayed_lockless

      def delayed_lockless?
        @delayed_lockless == true
      end

      # Runs only the delayed-job loop, and runs it without the master lock
      # (never returns).
      #
      # #run drives the schedule and the delayed queue from one loop in one
      # process, so a slow pass over a large dynamic schedule holds up delayed
      # jobs queued behind it. Splitting the two into separate processes keeps
      # delayed throughput off the scheduler's critical path.
      #
      # Dropping the master lock is safe because
      # #enqueue_items_in_batch_for_timestamp does its work inside a
      # WATCH/MULTI on the timestamp bucket and reports a lost race by
      # returning -1, so several processes can drain the same timestamp
      # without enqueueing anything twice.
      def run_delayed_only
        procline 'Starting Delayed'

        # trap signals
        register_signal_handlers

        # Quote from the resque/worker.
        # Fix buffering so we can `rake resque:scheduler > scheduler.log` and
        # get output from the child in there.
        $stdout.sync = true
        $stderr.sync = true

        self.delayed_lockless = true

        begin
          @th = Thread.current

          loop do
            begin
              handle_delayed_items
            rescue *INTERMITTENT_ERRORS => e
              log! e.message
            end
            poll_sleep
          end

        rescue Interrupt
          log 'Exiting'
        end
      end

      # Runs only the schedule (never returns): loads the schedule, keeps it
      # current when dynamic, and leaves the delayed queue to
      # #run_delayed_only. Holds the master lock, so exactly one of these
      # enqueues recurring jobs.
      def run_scheduled_only
        procline 'Starting Scheduler'

        # trap signals
        register_signal_handlers

        $stdout.sync = true
        $stderr.sync = true

        was_master = nil

        begin
          @th = Thread.current

          loop do
            begin
              # Check on changes to master/child
              @am_master = master?
              if am_master != was_master
                procline am_master ? 'Master scheduler' : 'Child scheduler'

                # Load schedule because changed
                reload_schedule!
              end

              update_schedule if am_master && dynamic
              was_master = am_master
            rescue *INTERMITTENT_ERRORS => e
              log! e.message
              release_master_lock
            end
            poll_sleep
          end

        rescue Interrupt
          log 'Exiting'
        end
      ensure
        release_master_lock
      end

      # Runs one batch unless this process has no business doing so: the delayed
      # loop may be running lockless (see #run_delayed_only), and otherwise the
      # master lock decides. -1 means stop, the same value a lost batch
      # transaction returns.
      def enqueue_batch_or_stop(timestamp, batch_size)
        return -1 if !delayed_lockless? && !am_master

        enqueue_items_in_batch_for_timestamp(timestamp, batch_size)
      end

      # Schedules one entry with rufus and records it. A schedule rufus cannot
      # parse would otherwise take down the whole scheduler on boot, taking
      # every other entry with it. Returns whether the entry was scheduled.
      def schedule_job_safely(name, interval_type, args, &block)
        @scheduled_jobs[name] = rufus_scheduler.send(interval_type, *args, &block)
        true
      rescue => e
        log_error "[Bad Schedule] ignoring #{name} with: " \
                  "#{e.message}\n#{e.backtrace.join("\n")}"
        false
      end

      # Counts what the schedule fires, per job class. Host apps that define
      # StatsTracker get the metric; everyone else gets nothing. Never let
      # instrumentation stop a scheduled job from being queued.
      #
      # The metric name has been misspelled since 2022. Kept as-is so existing
      # dashboards keep resolving; renaming it is its own change, alongside them.
      def track_recurring_enqueue(config)
        return if !defined?(StatsTracker)

        StatsTracker.increment(
          'ResqueScheuler.enqueue',
          tags: ["class_name:#{config['class']}"]
        )
      rescue => e
        log! e.inspect
      end
    end
  end
end
