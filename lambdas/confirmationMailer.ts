import { DynamoDBStreamHandler } from "aws-lambda";
import { SES_EMAIL_FROM, SES_EMAIL_TO, SES_REGION } from "env";
import { SESClient, SendEmailCommand, SendEmailCommandInput } from "@aws-sdk/client-ses";

if(!SES_EMAIL_TO || !SES_EMAIL_FROM || !SES_REGION) {
    throw new Error(
        "Please add the SES_EMAIL_TO, SES_EMAIL_FROM and SES_REGION environment variables in and env.js file located in the root directory"
    );
}

type ContactDetails = {
    name: string;
    email: string;
    message: string;
};

const client = new SESClient({region: SES_REGION});

export const handler: DynamoDBStreamHandler = async(event: any) => {
    console.log("Event - ", JSON.stringify(event));
    for(const record of event.Records) {
        console.log("Record - ", JSON.stringify(record));
        if(record.eventName === "INSERT" && record.dynamodb.NewImage) {
            const srcKey = record.dynamodb.NewImage.imageId.S;
            console.log(`Emailing user for adding ${srcKey}`);
            try {
                const { name, email, message }: ContactDetails = {
                    name: "The Photo Album",
                    email: SES_EMAIL_FROM,
                    message: `We recieved your Image. Its URL is s3://${process.env.BUCKET_NAME}/${srcKey}`,
                };
                const params = sendEmailParams({ name, email, message });
                await client.send(new SendEmailCommand(params));
                console.log(`Email sent`);
            } catch (error: unknown) {
                console.log("ERROR is: ", error);
            }
        }
    }
};

function sendEmailParams({ name, email, message }: ContactDetails) {
    const parameters: SendEmailCommandInput = {
        Destination: {
            ToAddresses: [SES_EMAIL_TO],
        },
        Message: {
            Body: {
                Html: {
                    Charset: "UTF-8",
                    Data: getHtmlContent({ name, email, message }),
                },
            },
            Subject: {
                Charset: "UTF-8",
                Data: `New image Upload`,
            },
        },
        Source: SES_EMAIL_FROM
    };
    return parameters;
}

function getHtmlContent({ name, email, message }: ContactDetails) {
    return `
        <html>
            <body>
                <h2>Sent from: </h2>
                <ul>
                    <li style="font-size:18px">👤 <b>${name}</b></li>
                    <li style="font-size:18px">✉️ <b>${email}</b></li>
                </ul>
                <p style="font-size:18px">${message}</p>
            </body>
        </html>
    `;
}