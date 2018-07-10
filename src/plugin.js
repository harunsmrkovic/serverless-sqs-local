const _ = require("lodash");
const { resolve } = require('path')

const SQS_ENDPOINT = process.env.SQS_ENDPOINT || "http://localhost:9324";

class SqsLocalPlugin {
  constructor(serverless, options, aws) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.log = message =>
      serverless.cli.log.bind(serverless.cli)(`SQS - ${message}`);
    this.options = options;
    this.aws = aws;
    this.sqs = new aws.SQS({
      endpoint: new aws.Endpoint(SQS_ENDPOINT),
      accessKeyId: "na",
      secretAccessKey: "na",
      region: "eu-west-1"
    });

    const offlineConfig = this.serverless.service.custom["serverless-offline"] || {}
    this.location = process.cwd()
    if (offlineConfig.location) {
        this.location = process.cwd() + "/" + offlineConfig.location
    } else if (this.serverless.config.servicePath) {
        this.location = this.serverless.config.servicePath
    }
  }

  start () {
    this.log('Subscribing to SQS endpoints...')
    this.subscribeAll()
  }

  subscribeAll () {
    const getARN = (event) => typeof event.sqs === 'string' ? event.sqs : event.sqs.arn
    const fns = Object.keys(this.service.functions)
      .map(fnName => {
        const fn = this.service.functions[fnName]
        return fn.events
          .filter(event => event.sqs != null)
          .map(event => ({ fn, event: getARN(event) }))
      })
      .map(t => t[0])
      .filter(t => t)

    this.sqs.listQueues({}, (err, qs) => {
      if (err) this.log(`Failed to list queues ${err.toString()}`)

      qs.QueueUrls.forEach(url => {
        const queueName = url.substr(url.lastIndexOf('/') + 1)

        fns
          .filter(({event}) => event.substr(event.lastIndexOf(':') + 1) === queueName)
          .forEach(({fn}) => {
            this.listenToQueue(url, this.createHandler(fn))
          })
      })
    })
  }

  listenToQueue (url, cb) {
    this.log(`Setting listen @ ${url}`)
    this.sqs.receiveMessage({
      QueueUrl: url,
      MessageAttributeNames: ['All'],
      WaitTimeSeconds: 20 // long poll lol
    }, (err, data) => {
      if (err) return this.log(`Cannot receive messages from ${url}; ${err.toString()}`)

      if (data.Messages) {
        this.log(`Received message @ queue ${url}, spinning up respective lambda`)
        cb({
          Records: data.Messages.map(this.wrapSQSMessage)
        },
        {},
        (err, data) => {
          if (err) {
            return this.log(`Lambda terminated with error ${err.toString()}`)
          }
          this.log(`Lambda returned data ${data.toString()}`)
        }
      )
      } else {
        this.log(`There are no new messages @ queue ${url}`)
      }

      this.listenToQueue(url, cb)
    })
  }

  wrapSQSMessage (rawMessage) {
    return _.keys(rawMessage).reduce((acc, key) => ({ ...acc, [_.camelCase(key)]: rawMessage[key] }), {})
  }

  createHandler(fn) {

    // use the main serverless config since this behavior is already supported there
    if (!this.serverless.config.skipCacheInvalidation || Array.isArray(this.serverless.config.skipCacheInvalidation)) {
      for (const key in require.cache) {

        // don't invalidate cached modules from node_modules ...
        if (key.match(/node_modules/)) {
            continue;
        }

        // if an array is provided to the serverless config, check the entries there too
        if (Array.isArray(this.serverless.config.skipCacheInvalidation) &&
            this.serverless.config.skipCacheInvalidation.find(pattern => new RegExp(pattern).test(key))) {
            continue;
        }

        delete require.cache[key];
      }
    }

    const handlerFnNameIndex = fn.handler.lastIndexOf(".");
    const handlerPath = fn.handler.substring(0, handlerFnNameIndex);
    const handlerFnName = fn.handler.substring(handlerFnNameIndex + 1);
    const fullHandlerPath = resolve(this.location, handlerPath);
    this.log(`require(${fullHandlerPath})[${handlerFnName}]`);
    const handler = require(fullHandlerPath)[handlerFnName];
    return handler;
  }

  getCommands() {
    return {
      sqs: {
        commands: {
          migrate: {
            lifecycleEvents: ["migrateHandler"],
            usage: "Creates queues from the current Serverless configuration."
          },
          remove: {
            lifecycleEvents: ["removeHandler"],
            usage:
              "Removes all queues listed in the current Serverless configuration."
          }
        }
      }
    };
  }

  getHooks() {
    return {
      'before:offline:start:init': () => this.start(),
      "sqs:migrate:migrateHandler": this.migrateHandler.bind(this),
      "sqs:remove:removeHandler": this.removeHandler.bind(this)
    };
  }

  migrateHandler() {
    this.log("Migrating...");
    const queues = this.getQueues();

    return this.createQueues(queues).then(created => {
      created.forEach(x => this.log(`Created queue at ${x}`));
    });
  }

  removeHandler() {
    this.log("Removing queues...");
    const queuesInConfiguration = this.getQueues();

    return this.removeQueues(queuesInConfiguration)
      .then(removed => {
        removed
          .filter(n => n)
          .forEach(removedQueue => this.log(`Deleted ${removedQueue}`));
      })
      .catch(err => this.log(err));
  }

  getQueues() {
    let stacks = [];

    const defaultStack = this.getDefaultStack();
    if (defaultStack) {
      stacks.push(defaultStack);
    }

    if (this.hasAdditionalStacksPlugin()) {
      stacks = stacks.concat(this.getAdditionalStacks());
    }

    return stacks
      .map(stack => this.getQueueDefinitionsFromStack(stack))
      .reduce((queues, queuesInStack) => queues.concat(queuesInStack), []);
  }

  createQueues(queues = []) {
    return Promise.all(queues.map(queue => this.createQueue(queue)));
  }

  resolveDeadLetterTargetArn(redrivePolicy) {
    return this.resolve(redrivePolicy.deadLetterTargetArn);
  }

  createQueue({
    QueueName,
    ContentBasedDeduplication,
    DelaySeconds,
    FifoQueue,
    MaximumMessageSize,
    MessageRetentionPeriod,
    ReceiveMessageWaitTimeSeconds,
    RedrivePolicy,
    VisibilityTimeout
  }) {
    let attributes = {
      ContentBasedDeduplication,
      DelaySeconds,
      FifoQueue,
      MaximumMessageSize,
      MessageRetentionPeriod,
      ReceiveMessageWaitTimeSeconds,
      RedrivePolicy,
      VisibilityTimeout
    };

    const deadLetterTargetArn = _.get(
      attributes,
      "RedrivePolicy.deadLetterTargetArn",
      ""
    );
    if (_.isObject(deadLetterTargetArn)) {
      this.log(
        `Omitting RedrivePolicy for ${QueueName} as the serverless-sqs-local plugin is not able to resolve ${Object.keys(
          deadLetterTargetArn
        )[0]} 😞\nSee https://github.com/Nevon/serverless-sqs-local/issues/1`
      );
      attributes = _.omit(attributes, "RedrivePolicy");
    }

    attributes = _.reduce(
      attributes,
      (acc, key, val) => {
        if (val !== undefined) {
          acc[key] = `${JSON.stringify(val)}`;
          return acc;
        }
      },
      {}
    );

    return new Promise((resolve, reject) =>
      this.sqs.createQueue(
        { QueueName, Attributes: attributes },
        (err, data) => (err ? reject(err) : resolve(data.QueueUrl))
      )
    );
  }

  removeQueues(queues = []) {
    return Promise.all(queues.map(queue => this.removeQueue(queue)));
  }

  removeQueue(queue) {
    return this.getQueueUrl(queue.QueueName)
      .then(url => {
        return new Promise((resolve, reject) => {
          this.sqs.deleteQueue(
            {
              QueueUrl: url
            },
            err => {
              if (err) {
                reject(err);
              } else {
                resolve(queue.QueueName);
              }
            }
          );
        });
      })
      .catch(err => {
        if (err.code === "AWS.SimpleQueueService.NonExistentQueue") {
          this.log(`Queue ${queue.QueueName} does not exist. Skipping.`);
        }
      });
  }

  getQueueUrl(queueName) {
    return new Promise((resolve, reject) => {
      this.sqs.getQueueUrl(
        {
          QueueName: queueName,
          QueueOwnerAWSAccountId: "na"
        },
        (err, data) => {
          if (err) {
            reject(err);
          } else {
            resolve(data.QueueUrl);
          }
        }
      );
    });
  }

  getDefaultStack() {
    return _.get(this.service, "resources");
  }

  getAdditionalStacks() {
    return _.values(_.get(this.service, "custom.additionalStacks", {}));
  }

  hasAdditionalStacksPlugin() {
    return _.get(this.service, "plugins", []).includes(
      "serverless-plugin-additional-stacks"
    );
  }

  getQueueDefinitionsFromStack(stack) {
    const resources = _.get(stack, "Resources", []);
    return Object.keys(resources)
      .map(key => {
        if (resources[key].Type === "AWS::SQS::Queue") {
          return resources[key].Properties;
        }
      })
      .filter(n => n);
  }
}

module.exports = SqsLocalPlugin;
