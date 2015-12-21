#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <marlin/marlin.h>
#include <sys/time.h>

int keySize = 100;
int valueSize = 100;

#define MDEBUG
#ifdef MDEBUG
int debug_on = 1;
#else
int debug_on = 0;
#endif
int inf_on = 1;

/* per second */
#define STARTING_RATE 10

/* special code from UI telling us to failover */
#define FAIL_OVER_CODE 999
#define FAIL_BACK_CODE 998

/* path to the python script that sends metrics to opentsdb */
#define METRIC_SENDER_PATH "/home/mapr/html/msend.py"

/* debug messages */
#define DPRINTF(...) if (debug_on) { fprintf(stderr, __VA_ARGS__); }

/* informational during normal operation */
#define IPRINTF(...) if (inf_on) { fprintf(stderr, __VA_ARGS__); }

/*
 * Utility Functions
 * -----------------
 */
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

/* This is a producer callback function. Callbacks are required,
 * so that producers are notified when memory held by records
 * can be reclaimed.
 */
void
producerCallback(int32_t err,
    marlin_producer_record_t record,
    int partitionid,
    int64_t offset,
    void *ctx)
{
	char *keyp, *valp;
	uint32_t ks, vs;
	int ret_val;

	/*
	DPRINTF("Producer Callback : \n");
	DPRINTF("Partition ID"
	    ": %d \n", partitionid);
	DPRINTF("Message Offset"
	    ": %d \n", offset);
	*/
	    
	ret_val = marlin_producer_record_get_key(record,
	    (const void **)&keyp, &ks);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("get_key() failed\n");
	}
	ret_val = marlin_producer_record_get_value(record,
	    (const void **)&valp, &vs);

	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("get_value() failed\n");
	}

	/* free the key and value buffers */
	free(keyp);
	free(valp);

	ret_val = marlin_producer_record_destroy(record);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("destroy failed\n");
	}
}

/*
 * Initialize a producer
 * ---------------------
 */
int
producer_init(const char *fullTopicName,
		marlin_topic_partition_t *tp, marlin_config_t *confp, marlin_producer_t *prodp)
{
	int ret_val;

	DPRINTF("\ninitializing producer");
	/* Create a topic partition, which is an object that specifies
	 * which stream and topic to publish messages to. It is not
	 * the same as an actual partition in a topic. Producers
	 * cannot add partitions to topics.
	 */

	DPRINTF("\nSTEP 1: Creating a topic partition... %s \n",
	    fullTopicName);

	ret_val = marlin_topic_partition_create(
	    fullTopicName, 0, tp);

	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_topic_partition_create() failed\n");
		return (ret_val);
	}

	/* Create a config, which you can use to set non-default
	 * values for configuration parameters for the producer.
	 */
	DPRINTF("\nSTEP 2: Creating a config...\n");

	ret_val = marlin_config_create(confp);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_config_create() failed\n");
		return (ret_val);
	}

	/* Set a configuration parameter. Here, the code
	 * demonstrates how to set a value. However, the value
	 * set is simply the default for the specified configuration
	 * parameter.
	 */
	ret_val = marlin_config_set(*confp, "buffer.memory", "33554432");
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_config_set() failed\n");
		return (ret_val);
	}
	ret_val = marlin_config_set(*confp, "marlin.buffer.max.time.ms", "500");
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_config_set() failed\n");
		return (ret_val);
	}
	
	/* Create a producer. */
	DPRINTF("\nSTEP 3: Creating a producer... \n");
	ret_val = marlin_producer_create(*confp, prodp);

	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("marlin_producer_create() failed\n");
		return (ret_val);
	}
	return (EXIT_SUCCESS);
}

int
producer_shutdown(marlin_topic_partition_t *topic,
		marlin_config_t *config, marlin_producer_t *producer)
{
	int ret_val;

	/* destroy the producer. */
	DPRINTF("\nDestroying the Producer... \n");
	ret_val = marlin_producer_destroy(*producer);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("producer destroy failed\n");
		return (ret_val);
	}

	/* destroy the topic partition */
	ret_val = marlin_topic_partition_destroy(*topic);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("partition destroy failed\n");
		return (ret_val);
	}

	/* destroy the config */
	DPRINTF("\nDestroying the config...\n");
	ret_val = marlin_config_destroy(*config);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("partition destroy failed\n");
		return (ret_val);
	}
	*topic = *config = *producer = 0;
	return (EXIT_SUCCESS);
}

int
send_metrics(const char *cur, const char *primary, const char *backup, int newrate)
{
	char namebuf1[100], namebuf2[100];
	int ret_val;

	if (cur == primary) {
		snprintf(namebuf1, 100, "%s producer.primary_rate %d", 
			METRIC_SENDER_PATH, newrate);
		snprintf(namebuf2, 100, "%s producer.backup_rate 0", 
			METRIC_SENDER_PATH);
	} else {
		snprintf(namebuf1, 100, "%s producer.backup_rate %d", 
				METRIC_SENDER_PATH, newrate);
		snprintf(namebuf2, 100, "%s producer.primary_rate 0", 
				METRIC_SENDER_PATH);
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

/*
 * Main Producer Loop
 * ------------------
 */
int
producer(const char *fullTopicName, const char *backupTopicName,
		const char *fname, const char *pipe_fname)
{
	int ret_val;
	char key_content[keySize];
	FILE *fp, *pipe_fp;
	int pipe_fd;
	char *line = NULL;
	size_t len = 0;
	ssize_t read;
	int msg_idx = 0;
	char *linebuf = NULL;
	char *keybuf = NULL;
	struct timeval now, last_now;
	int newrate, rate = STARTING_RATE;
	int n_sent_this_sec = 0;
	long tdiff;
	const char *cur_topic = fullTopicName;
	char namebuf[100];
	marlin_topic_partition_t topic;
	marlin_config_t config;
	marlin_producer_t producer;

	ret_val = producer_init(cur_topic, &topic, &config, &producer);
	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("producer_init() failed\n");
		return (ret_val);
	}

	fp = fopen(fname, "ro");
	if (fp == NULL) {
		DPRINTF("open of file %s failed\n", fname);
		return (-1);
	}
	DPRINTF("opening pipe %s\n", pipe_fname);
	pipe_fd = open(pipe_fname, O_RDWR|O_NONBLOCK);
	if (pipe_fd < 0) {
		DPRINTF("open of file %s failed\n", pipe_fname);
		return (-1);
	}
	pipe_fp = fdopen(pipe_fd, "r");

	DPRINTF("\nSTEP 4: The producer will create, send, "
	    "and flush now\n");

	/* send initial metrics */
	ret_val = send_metrics(cur_topic, fullTopicName, backupTopicName, rate);
	if (EXIT_SUCCESS != ret_val) {
		IPRINTF("send_metrics() failed\n");
		return (ret_val);
	}

	gettimeofday(&last_now, NULL);
	while ((read = getline(&line, &len, fp)) != -1) {
		/*
		 * check if we've sent enough,
		 * if so wait it out so we keep to the specified rate
		 */
		if (n_sent_this_sec >= rate) {

			/* send new metrics */
			ret_val = send_metrics(cur_topic, fullTopicName, backupTopicName, rate);
			if (EXIT_SUCCESS != ret_val) {
				IPRINTF("send_metrics() failed\n");
				return (ret_val);
			}

			gettimeofday(&now, NULL);
			tdiff = tvdiff(&last_now, &now);
			printf("time diff %lu rate %d\n", tdiff, rate);
			if (tdiff < 1000) {
				DPRINTF("produced %d messages in %lums, sleeping\n",
				    rate, tdiff);
				usleep((1000 - tdiff) * 1000);
			} else if (tdiff >= 1000) {
				DPRINTF("warning:  falling behind, took %lums to write"
				    " %d messages\n", tdiff, rate);
			}
			gettimeofday(&last_now, NULL);
			n_sent_this_sec = 0;

			/* check the pipe for a rate change */
			DPRINTF("checking pipe\n");
			ret_val = is_pipe_ready(fileno(pipe_fp));
			if (ret_val < 0) {
				IPRINTF("is_pipe_ready() failed\n");
				return (ret_val);
			}
			if (ret_val > 0) {
				(void) fscanf(pipe_fp, "%d", &newrate);
				IPRINTF("new rate: %d\n", newrate);

				/* if we need to failover, toggle connection to the other cluster */
				if (newrate == FAIL_OVER_CODE || newrate == FAIL_BACK_CODE) {
					IPRINTF("failing over to %s cluster\n",
					    newrate == FAIL_OVER_CODE ? "backup" : "primary");
					ret_val = producer_shutdown(&topic, &config, &producer);
					if (EXIT_SUCCESS != ret_val) {
						DPRINTF("trying to failover: shutdown() failed\n");
						return (ret_val);
					}
					cur_topic =
					    newrate == FAIL_OVER_CODE ? backupTopicName : fullTopicName;
					ret_val = producer_init(cur_topic, &topic, &config, &producer);
					if (EXIT_SUCCESS != ret_val) {
						DPRINTF("trying to failover: init() failed\n");
						return (ret_val);
					}
					/* this was an out-of-band code so leave rate unchanged */
				} else {
					/* this was a rate request, set the new rate to it */
					rate = newrate;

					/* send new metrics */
					ret_val =
					    send_metrics(cur_topic,
					    fullTopicName, backupTopicName, rate);
					if (EXIT_SUCCESS != ret_val) {
						IPRINTF("send_metrics() failed\n");
						return (ret_val);
					}
				}
			}
			continue;
		}
		printf("sending inc %d\n", n_sent_this_sec);
		n_sent_this_sec++;

		/* DPRINTF("Retrieved line of length %zu :\n", read); */
		/* DPRINTF("%s", line); */

		/* Create a unique key and value for each message. */
		char key_content[keySize];
		sprintf(key_content, "Key_%d", msg_idx);

		linebuf = malloc(strlen(line) + 1);
		keybuf = malloc(strlen(key_content) + 1);
		strcpy(keybuf, key_content);
		strcpy(linebuf, line);

		/* Create a record that contains the message. */
		marlin_producer_record_t record;
		ret_val = marlin_producer_record_create(
		    topic, keybuf, strlen(keybuf) + 1,
		    linebuf, strlen(linebuf) + 1, &record);

		if (EXIT_SUCCESS != ret_val) {
			DPRINTF("marlin_producer_record_create() failed\n");
			return (ret_val);
		}

		/* Buffer the message. */
		/* DPRINTF("calling marlin_producer_send()\n"); */
		ret_val =
		    marlin_producer_send(producer,
		    record, producerCallback, NULL);
	
		if (EXIT_SUCCESS != ret_val) {
			DPRINTF("marlin_producer_send() failed\n");
			return (ret_val);
		}
		msg_idx++;
	
		/* Flush the message.
		 * this is the synchronous approach but reduces throughput
		 *
		ret_val = marlin_producer_flush(producer);
		if (EXIT_SUCCESS != ret_val) {
			DPRINTF("marlin_producer_flush() failed\n");
			return (ret_val);
		}
		DPRINTF("Produced: MESSAGE %d: ", msg_idx);
		*/
	}
	fclose(fp);
	if (line)
		free(line);

	/* send 0 metric now that we're shuttong down */
	ret_val = send_metrics(cur_topic, fullTopicName, backupTopicName, 0);
	if (EXIT_SUCCESS != ret_val) {
		IPRINTF("send_metrics() failed\n");
		return (ret_val);
	}
	return (EXIT_SUCCESS);
}

/* MAIN */
int
main(int argc, char *argv[])
{
	char *maintopic, *backuptopic, *fname;
	char *pipename;
	int ret_val;

	if (argc != 5) {
		fprintf(stderr, 
		    "usage:  %s "
		    "/stream:topic /backup_stream:topic "
		    " <filename> <pipe_filename>\n", argv[0]);
		exit(-1);
	}
	maintopic = argv[1];
	backuptopic = argv[2];
	fname = argv[3];
	pipename = argv[4];

	/* Produce Messages */
	ret_val = producer(maintopic, backuptopic,
			fname, pipename);

	if (EXIT_SUCCESS != ret_val) {
		DPRINTF("\nFAIL: producer failed\n");
		exit(-1);
	}
	printf("done\n");
}
