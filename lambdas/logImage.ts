import { SQSHandler } from "aws-lambda";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb"
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const s3 = new S3Client();
const sqsClient = new SQSClient();
const ddbDocClient = createDdbDocClient();

export const handler: SQSHandler = async(event) => {
    console.log("Event - ", JSON.stringify(event));
    for(const record of event.Records) {
        const recordBody = JSON.parse(record.body);
        const snsMessage = JSON.parse(recordBody.Message)

        if(snsMessage.Records) {
            console.log("Record body - ", JSON.stringify(snsMessage));
            for(const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));
                const fileType = srcKey.split(".").pop() || "";
                const eventType = messageRecord.eventName;

                if(eventType === "ObjectCreated:Put") {
                    console.log(`Adding ${srcKey} to table`);
                    if (["jpeg", "png", "jpg"].includes(fileType)) {
                        console.log(`File type ${fileType} okay`);
                        try {
                            await ddbDocClient.send(
                                new PutCommand({
                                    TableName: process.env.TABLE_NAME,
                                    Item: {
                                        imageId: srcKey,
                                    }
                                })
                            )
                        } catch (error) {
                            console.log(`Failed to add ${srcKey} - ${error}`);
                        }
                    } else {
                        console.log(`Unsupported file type: ${fileType}`);
                        try {
                            recordBody.Error = `Unsupported file type: ${fileType}`
                            await sqsClient.send(
                                new SendMessageCommand({
                                    QueueUrl: process.env.DLQ_URL,
                                    MessageBody: JSON.stringify(recordBody)
                                })
                            );
                        } catch (error) {
                            console.log(`Failed to message DLQ - ${error}`);
                        }
                    }
                } else if(eventType === "ObjectRemoved:Delete") {
                    console.log(`Deleting ${srcKey}`);
                    try {
                        await ddbDocClient.send(
                            new DeleteCommand({
                                TableName: process.env.TABLE_NAME,
                                Key: {
                                    imageId: srcKey,
                                },
                            })
                        );
                        console.log(`Deleted ${srcKey}`);
                    } catch (error) {
                        console.log(`Failed to delete ${srcKey} - ${error}`);
                    }
                }
            }
        }
    }
}; 

function createDdbDocClient() {
    const ddbClient = new DynamoDBClient({region: process.env.REGION});
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = {marshallOptions, unmarshallOptions};
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}