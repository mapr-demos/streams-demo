#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <marlin/marlin.h>
#include <sys/time.h>

int debug_on = 0;
int inf_on = 1;
char *consname;

#define FAIL_OVER_CODE 999
#define FAIL_BACK_CODE 998

/* debug messages */
#define DPRINTF(...) if (debug_on) { fprintf(stderr, __VA_ARGS__); }

/* informational during normal operation */
#define IPRINTF(...) if (inf_on) { fprintf(stderr, __VA_ARGS__); }

/* path to the python script that sends metrics to opentsdb */
#define METRIC_SENDER_PATH "/home/mapr/msend.py"

/*
 * Utility Functions
 * -----------------
 */

int
send_metrics(const char *cur, const char *primary, const char *backup, int newrate)
{
	char namebuf1[100], namebuf2[100];
	int ret_val;

	if (cur == primary) {
		snprintf(namebuf1, 100, "%s consumer_%s.primary_rate %d", 
			METRIC_SENDER_PATH, consname, newrate);
		snprintf(namebuf2, 100, "%s consumer_%s.backup_rate 0", 
			METRIC_SENDER_PATH, consname);
	} else {
		snprintf(namebuf1, 100, "%s consumer_%s.backup_rate %d",
				METRIC_SENDER_PATH, consname, newrate);
		snprintf(namebuf2, 100, "%s consumer_%s.primary_rate 0",
				METRIC_SENDER_PATH, consname);
	}
	ret_val = system(namebuf1);
	if (ret_val < 0) {
		IPRINTF("system() metric sender failed\n");
		return (ret_val);
	}
	ret_val = system(namebuf2);
	if (ret_val < 0) {
		IPRINTF("system() metric sender failed\n");
		return (ret_val);
	}
	return (EXIT_SUCCESS);
}

long
tvdiff(struct timeval *stime, struct timeval *etime)
{
	long diffmsec;

	diffmsec = (etime->tv_sec-stime->tv_sec) * 1000;
	diffmsec += (etime->tv_usec-stime->tv_usec) / 1000;

	return (diffmsec);
}

int
is_pipe_ready(int fd)
{
	int rc;
	fd_set fds;
	struct timeval tv;

	FD_ZERO(&fds);
	FD_SET(fd,&fds);

	tv.tv_sec = tv.tv_usec = 0;
	rc = select(fd+1, &fds, NULL, NULL, &tv);
	if (rc < 0)
		return -1;

	return FD_ISSET(fd, &fds) ? 1 : 0;
}

int
consumer_shutdown(
		marlin_config_t *config, marlin_consumer_t *consumer)
{
	int ret_val = EXIT_SUCCESS;

	/* commit everything we've read */
	if (0 != marlin_consumer_commit_all(*consumer, 1)) {
		DPRINTF("error committing\n");
	} else {
		DPRINTF("commit successful\n");
	}

	/* Destroy the consumer. */
	DPRINTF("\nDestroying the consumer... \n");
	ret_val = marlin_consumer_destroy(*consumer);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_cons_d() failed\n");
		return (ret_val);
	}

	/* Destroy the config. */
	DPRINTF("\nSTEP 8: Destroy the config...\n");
	marlin_config_destroy(*config);
	DPRINTF("\n****** CONSUMER END ******\n");

	*config = *consumer = 0;
	return (ret_val);
}

int
consumer_init(const char *fullTopicName, const char *ftn2, const char *gid,
		marlin_config_t *confp, marlin_consumer_t *consp)
{
	int ret_val = EXIT_SUCCESS;
	const char *subs_topics[2];

	DPRINTF("\ninitializing consumer");
	/* Create a topic partition,
	 * which is an object that specifies
	 * which stream and topic to
	 * publish messages to. It is not
	 * the same as an actual
	 * partition in a topic. Producers
	 * cannot add partitions to topics.
	 */

	/* Create a config,
	 * which you can use to set non-default
	 * values for configuration parameters
	 * for the consumer.
	 */

	DPRINTF("\nSTEP 2: Creating a config...\n");
	ret_val = marlin_config_create(confp);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_config_create() " " failed\n");
		return (ret_val);
	}

	/* Set a non-default
	 * value for a configuration parameter.
	 */

	DPRINTF("\nSTEP 3: Setting the"
	       "config parameter auto.offset.reset to" "'earliest'.. \n");

	marlin_config_set(*confp, "auto.offset.reset", "earliest");
	marlin_config_set(*confp, "group.id", gid);

	/* Create a consumer. */
	DPRINTF("\nSTEP 3: Creating consumer... \n");
	ret_val = marlin_consumer_create(*confp, NULL, NULL, consp);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_consumer_create() " " failed\n");
		return (ret_val);
	}

	/* Subscribe the consumer to the topic. */
	DPRINTF("\nSTEP 4: Subscribing " " the consumer to the topic...\n");
	subs_topics[0] = fullTopicName;
	subs_topics[1] = ftn2;
	ret_val = marlin_consumer_subscribe_topics(*consp, subs_topics, 2);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("subsc_topics() failed\n");
		return (ret_val);
	}
	return (ret_val);
}

int
consumer(const char *fullTopicName, const char *ftn2,
		const char *backupTopicName,
		const char *btn2, const char *pipe_fname)
{
	int ret_val;
	int code;
	marlin_config_t config;
	marlin_consumer_t consumer;
	struct timeval now, last_now;
	const char *cur_topic = fullTopicName;
	int tot = 0, last_tot = 0;
	FILE *pipe_fp;
	int pipe_fd;
	long tdiff;
	
	ret_val = consumer_init(cur_topic,
			cur_topic == fullTopicName ? ftn2 : btn2,
			cur_topic == fullTopicName ? "1" : "1", &config, &consumer);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("consumer_init() failed\n");
		return (ret_val);
	}

	DPRINTF("opening pipe %s\n", pipe_fname);
	pipe_fd = open(pipe_fname, O_RDWR|O_NONBLOCK);
	if (pipe_fd < 0) {
		DPRINTF("open of file %s failed\n", pipe_fname);
		return (-1);
	}
	pipe_fp = fdopen(pipe_fd, "r");

	/* Poll for
	 * messages
	 * continuously. Process them and print
	 them to standard output. */
	DPRINTF("\nSTEP 6: Polling for messages:\n");
	gettimeofday(&last_now, NULL);
	for(;;) {
		marlin_consumer_record_t *records;
		long poll_time_out = 1000;
		uint32_t nRecords;
		int d;
		float fracs_of_sec;
		int rate;

		gettimeofday(&now, NULL);
		tdiff = tvdiff(&last_now, &now);
		if (tdiff >= 1000) {
			fracs_of_sec  = tdiff / 1000.0;
			d = tot - last_tot;
			rate = d / fracs_of_sec;
			IPRINTF("f = %f, d = %d, r = %d\n", fracs_of_sec, d, rate);

			ret_val = send_metrics(cur_topic,
			    fullTopicName, backupTopicName, rate);
			if (EXIT_SUCCESS != ret_val) {
				IPRINTF("send_metrics() failed\n");
				return (ret_val);
			}
			gettimeofday(&last_now, NULL);
			last_tot = tot;
		}

		/* check the command pipe to see if we have something */
		DPRINTF("checking pipe\n");
		ret_val = is_pipe_ready(fileno(pipe_fp));
		if (ret_val < 0) {
			DPRINTF("is_pipe_ready() failed\n");
			return (ret_val);
		}
		if (ret_val > 0) {
			(void) fscanf(pipe_fp, "%d", &code);

			/* if we need to failover, toggle connection to the other cluster */
			if (code != FAIL_OVER_CODE && code != FAIL_BACK_CODE) {
				DPRINTF("got weird code %d, bailing\n", code);
				return (-1);
			}

			IPRINTF("failing over to %s cluster\n",
			    code == FAIL_OVER_CODE ? "backup" : "primary");

			ret_val = consumer_shutdown(&config, &consumer);
			if (EXIT_SUCCESS != ret_val) {
				DPRINTF("trying to failover: shutdown() failed\n");
				return (ret_val);
			}
			cur_topic =
			    code == FAIL_OVER_CODE ? backupTopicName : fullTopicName;
			ret_val = consumer_init(cur_topic,
			    cur_topic == fullTopicName ? ftn2 : btn2,
			    cur_topic == fullTopicName ? "1" : "1",
			    &config, &consumer);
			if (EXIT_SUCCESS != ret_val) {
				DPRINTF("trying to failover: init() failed\n");
				return (ret_val);
			}
		}

		/* now poll for messages */
		ret_val =
		    marlin_consumer_poll(consumer,
					 poll_time_out, &records, &nRecords);

		if (EXIT_SUCCESS != ret_val) {
			DPRINTF("cons_poll() fail\n");
			return (ret_val);
		}
		/* Get the # of
		 * messages in each record. */
		for (int rec = 0; rec < nRecords; ++rec) {
			uint32_t nummsgs_c;
			ret_val =
			    marlin_consumer_record_get_message_count(records
								     [rec],
								     &nummsgs_c);
			if (EXIT_SUCCESS != ret_val) {
				DPRINTF("cons_grc()\n");
				return (ret_val);
			}
			uint32_t key_size_c;
			void *key_c;
			uint32_t value_size_c;
			void *value_c;

			for (uint32_t i = 0; i < nummsgs_c; ++i) {
				/* Get the message key. */
				ret_val =
				    marlin_msg_get_key(*records,
						       i, &key_c, &key_size_c);
				if (EXIT_SUCCESS != ret_val) {
					DPRINTF("marlin_mgk() failed\n");
					return (ret_val);
				}
				/* get msg val. */
				ret_val =
				    marlin_msg_get_value(*records, i, &value_c,
							 &value_size_c);
				if (EXIT_SUCCESS != ret_val) {
					DPRINTF("msg_gv()" " failed\n");
					return (ret_val);
				}
				printf("%s", (char *)value_c);
				DPRINTF("Consumed: MESSAGE %d "
				       " (Key: %s Value: %s )\n", i,
				       (char *)key_c, (char *)value_c);
				tot++;
			}
		}
		DPRINTF("committing...\n");
		if (0 != marlin_consumer_commit_all(consumer, 1)) {
			DPRINTF("error committing\n");
		} else {
			DPRINTF("commit successful\n");
		}
	}
	return (EXIT_SUCCESS);
}

/* MAIN */
int
main(int argc, char *argv[])
{
	int ret_val;
	char *fullTopicName;
	char *maintopic, *backuptopic;
	char *maintopic2, *backuptopic2;
	char *pipename;

	if (argc != 7) {
		fprintf(stderr, 
		    "usage:  %s "
		    "/stream1:topic /stream2:topic "
		    "/backup_stream1:topic /backup_stream2:topic "
		    " <pipe_filename> <name_for_metrics>\n", argv[0]);
		exit(-1);
	}

	maintopic = argv[1];
	maintopic2 = argv[2];
	backuptopic = argv[3];
	backuptopic2 = argv[4];
	pipename = argv[5];
	consname = argv[6];

	ret_val = consumer(maintopic, maintopic2, backuptopic, backuptopic2, pipename);

	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("\nFAIL: consumer failed\n");
		exit(-1);
	}
}
