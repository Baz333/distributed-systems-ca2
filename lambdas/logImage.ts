import { SQSHandler } from "aws-lambda";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb"
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const s3 = new S3Client();
const sqsClient = new SQSClient();
const ddbDocClient = createDdbDocClient();

export const handler: SQSHandler = async(event) => {
    console.log("Event ", JSON.stringify(event));
    for(const record of event.Records) {
        const recordBody = JSON.parse(record.body);
        const snsMessage = JSON.parse(recordBody.Message)

        if(snsMessage.Records) {
            console.log("Record body ", JSON.stringify(snsMessage));
            for(const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));
                const fileType = srcKey.split(".").pop() || "";

                if (["jpeg", "png", "jpg"].includes(fileType)) {
                    console.log(`File type okay: ${fileType}`)
                    try {
                        const filename = srcKey.substring(srcKey.lastIndexOf("/") + 1); //Remove folders from key

                        await ddbDocClient.send(
                            new PutCommand({
                                TableName: process.env.TABLE_NAME,
                                Item: {
                                    imageId: filename,
                                }
                            })
                        )
                    } catch (error) {
                        console.log(error);
                    }
                } else {
                    console.log("Incorrect file type")
                    try {
                        recordBody.Error = `Unsupported file type: ${fileType}`
                        await sqsClient.send(
                            new SendMessageCommand({
                                QueueUrl: process.env.DLQ_URL,
                                MessageBody: JSON.stringify(recordBody)
                            })
                        );
                    } catch (error) {
                        console.log("ERROR: " + error);
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