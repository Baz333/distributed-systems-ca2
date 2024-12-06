import { SNSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb"

const ddbDocClient = createDdbDocClient();

export const handler = async(event: SNSEvent) => {
    console.log("Event ", JSON.stringify(event));
    for(const record of event.Records) {
        const sns = record.Sns;
        const snsMessage = JSON.parse(sns.Message);
        const metadata = sns.MessageAttributes.metadata_type.Value;
        console.log("MESSAGE ID: ", snsMessage.id);
        console.log("MESSAGE VALUE: ", snsMessage.value);

        if(!metadata) {
            throw new Error(
                "No metadata"
            );
        }

        if(!["Caption", "Date", "Photographer"].includes(metadata)) {
            throw new Error(
                "Invalid metadata type"
            );
        }

        try {
            console.log(`Updating ${snsMessage.id}`);
            await ddbDocClient.send(
                new UpdateCommand({
                    TableName: process.env.TABLE_NAME,
                    Key: {
                        imageId: snsMessage.id
                    },
                    UpdateExpression: "SET #m = :v",
                    ExpressionAttributeNames: {
                        "#m": metadata,
                    },
                    ExpressionAttributeValues: {
                        ":v": snsMessage.value,
                    }
                })
            );
            console.log(`Updated ${snsMessage.id}`);
        } catch (error) {
            console.log(`Failed to update ${snsMessage.id}`);
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