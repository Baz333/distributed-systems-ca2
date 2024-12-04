import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";

import { Construct } from "constructs";
import { DynamoDB } from "@aws-sdk/client-dynamodb";
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class CA2AppStack extends cdk.Stack {
	constructor(scope: Construct, id: string, props?: cdk.StackProps) {
		super(scope, id, props);

		//S3 Bucket
		const imageBucket = new s3.Bucket(this, "ImageUploadBucket", {
			removalPolicy: cdk.RemovalPolicy.DESTROY,
			autoDeleteObjects: true,
			publicReadAccess: false,
		});

		//SNS Topic
		const imageUploadTopic = new sns.Topic(this, "ImageUploadTopic", {
			displayName: "Image upload topic",
		});

		//SQS Queues
		const deadLetterQueue = new sqs.Queue(this, "DeadLetterQueue", {
			retentionPeriod: cdk.Duration.minutes(10)
		});

		const imageQueue = new sqs.Queue(this, "ImageUploadQueue", {
			deadLetterQueue: {
				queue: deadLetterQueue,
				maxReceiveCount: 2,
			},
		});

		//DynamoDB Table
		const imageTable = new dynamodb.Table(this, "ImageTable", {
			partitionKey: {
				name: "imageId",
				type: dynamodb.AttributeType.STRING,
			},
			removalPolicy: cdk.RemovalPolicy.DESTROY,
		});

		//Lambda Functions
		const appCommonFnProps = {
			runtime: lambda.Runtime.NODEJS_18_X,
			memorySize: 1024,
			timeout: cdk.Duration.seconds(3),
		}

		const logImageFn = new lambdanode.NodejsFunction(this, "LogImageFn", {
			...appCommonFnProps,
			entry: `${__dirname}/../lambdas/logImage.ts`,
			environment: {
				TABLE_NAME: imageTable.tableName,
				REGION: cdk.Aws.REGION,
				DLQ_URL: deadLetterQueue.queueUrl,
			},
		});

		const confirmationMailerFn = new lambdanode.NodejsFunction(this, "ConfirmationMailerFn", {
			...appCommonFnProps,
			entry: `${__dirname}/../lambdas/confirmationMailer.ts`,
		});

		const rejectionMailerFn = new lambdanode.NodejsFunction(this, "RejectionMailerFn", {
			...appCommonFnProps,
			entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
		});
		
		const updateTableFn = new lambdanode.NodejsFunction(this, "UpdateTableFn", {
			...appCommonFnProps,
			entry: `${__dirname}/../lambdas/updateTable.ts`,
			environment: {
				TABLE_NAME: imageTable.tableName,
			},
		})

		//S3 -> SNS
		imageBucket.addEventNotification(
			s3.EventType.OBJECT_CREATED,
			new s3n.SnsDestination(imageUploadTopic)
		);

		//SNS -> SQS
		imageUploadTopic.addSubscription(
			new subs.SqsSubscription(imageQueue)
		);

		//SNS -> Lambda
		imageUploadTopic.addSubscription(
			new subs.LambdaSubscription(confirmationMailerFn)
		);

		imageUploadTopic.addSubscription(
			new subs.LambdaSubscription(updateTableFn)
		);

		//SQS -> Lambda
		const logImageEventSource = new events.SqsEventSource(imageQueue, {
			batchSize: 5,
			maxBatchingWindow: cdk.Duration.seconds(5),
		});
		logImageFn.addEventSource(logImageEventSource);

		//DLQ -> Lambda
		const rejectionMailerEventSource = new events.SqsEventSource(deadLetterQueue, {
			batchSize: 5,
			maxBatchingWindow: cdk.Duration.seconds(5),
		});
		rejectionMailerFn.addEventSource(rejectionMailerEventSource);

		//Permissions
		imageBucket.grantRead(logImageFn);
		imageTable.grantWriteData(logImageFn);
		imageTable.grantReadWriteData(updateTableFn);

		confirmationMailerFn.addToRolePolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: [
					"ses:SendEmail",
					"ses:SendRawEmail",
					"ses:SendTemplatedEmail",
				],
				resources: ["*"],
			})
		);

		rejectionMailerFn.addToRolePolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: [
					"ses:SendEmail",
					"ses:SendRawEmail",
					"ses:SendTemplatedEmail",
				],
				resources: ["*"],
			})
		);

		logImageFn.addToRolePolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: [
					"sqs:SendMessage",
				],
				resources: ["*"],
			})
		)

		// Output
		new cdk.CfnOutput(this, "bucketName", {
			value: imageBucket.bucketName,
		});
	}
}
