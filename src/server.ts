import Fastify from 'fastify';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import { randomUUID } from "node:crypto";
import { fromIni } from "@aws-sdk/credential-providers";
import { S2SBidirectionalStreamClient, StreamSession } from './nova-client';
import {mulaw} from 'alawmulaw';
import { Twilio, twiml } from "twilio"
import { readFileSync } from 'node:fs';

//read the audio bytes from hello.pcm file
const helloAudioBytes = readFileSync('audio/hello.pcm');

// Find your Account SID and Auth Token at twilio.com/console
// and set the environment variables. See http://twil.io/secure

const apiSid = process.env.TWILIO_API_SID;
const apiSecret = process.env.TWILIO_API_SECRET;
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const sipEndpoint = process.env.SIP_ENDPOINT;

const fromNumber = process.env.TWILIO_FROM_NUMBER;
const toNumber = process.env.TWILIO_VERIFIED_CALLER_ID;

const twClient = new Twilio(apiSid, apiSecret, {accountSid});


const SYSTEM_PROMPT = "You are a customer service agent for a car rental company, \"The Car Genie\". The user and you will engage in a spoken " +
"dialog exchanging the transcripts of a natural real-time conversation. Keep your responses professional, polite and short, " +
"generally two or three sentences for chatty scenarios." + 
"You are here to answer questions related to the car rentals:" + 
"Customers would call regarding their car rental bookings, " + 
"like status of the rental, cancellations, extensions etc. " + 
"They would also reach out for any car rental policy related questions" + 
"At all costs, avoid answering any general real world questions and any other questions which are out-of-context and not related to \"The Car Genie\" car rental company" + 
"Furthermore, for any booking related actions, including status, please confirm with the customer before invoking the tools" + 
"Make sure you greet the customer and tell about yourself as welcome message as soon as the audio is started";

// Configure AWS credentials
const AWS_PROFILE_NAME = process.env.AWS_PROFILE ?? 'bedrock-test';

// Create the AWS Bedrock client
const bedrockClient = new S2SBidirectionalStreamClient({
    requestHandlerConfig: {
        maxConcurrentStreams: 10,
    },
    clientConfig: {
        region: process.env.AWS_REGION || "us-east-1",
        credentials: fromIni({ profile: AWS_PROFILE_NAME })
    }
});


const sessionMap = {};

const sipTwiml = `
<Response>
    <Say>Hang on for a moment while I forward the call to an agent</Say>
    <Pause length="1"/>
    <Dial>
    <Sip>${sipEndpoint}</Sip>
</Dial>
</Response>
`;

// Initialize Fastify
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Root Route
fastify.get('/', async (request, reply) => {
    reply.send({ message: 'Twilio Media Stream Server is running!' });
});


// Route to initiate outbound calls to a phone number from Twilio
// Invoke this endpoint to initiate an outbound call
fastify.all('/outbound-call', async (request, reply) => {    

    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
                          <Response>
                              <Say language="en-US" voice="Polly.Matthew-Generative">Please wait while we connect your call to Amazon Nova Sonic A.I. speech to speech system </Say>
                              <Pause length="1"/>
                              <Say>O.K. you can start talking!</Say>
                              <Connect>
                                <Stream url="wss://${request.headers.host}/media-stream" />
                              </Connect>
                          </Response>`;
    

    const call = await twClient.calls.create({
        from: fromNumber,
        to: toNumber,
        twiml: twimlResponse,
      });

      reply.type('text/plain').send("Ok");

});


// Route for Twilio to handle incoming and outgoing calls
// <Say> punctuation to improve text-to-speech translation
fastify.all('/incoming-call', async (request, reply) => {    

    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
                          <Response>
                              <Say language="en-US" voice="Polly.Matthew-Generative">Please wait while we connect your call to Amazon Nova Sonic A.I. speech to speech system </Say>
                              <Connect>
                                <Stream url="wss://${request.headers.host}/media-stream" />
                              </Connect>
                          </Response>`;
    reply.type('text/xml').send(twimlResponse);
});


// Route for Twilio to handle incoming and outgoing calls
// <Say> punctuation to improve text-to-speech translation
fastify.all('/failover', async (request, reply) => {

    reply.type('text/xml').send(sipTwiml);
});


// WebSocket route for media-stream
fastify.register(async (fastify) => {
    fastify.get('/media-stream', { websocket: true }, (connection, req) => {
        console.log('Client connected');

        //create a session
        const sessionId = randomUUID();   
        const session: StreamSession = bedrockClient.createStreamSession(sessionId);
        sessionMap[sessionId] = session //store the session in the map
        bedrockClient.initiateSession(sessionId) //initiate the session

        let callSid = '';
        // Handle incoming messages from Twilio
        connection.on('message', async (message) => {

            try {                
                const data = JSON.parse(message);
                //use streamSid as session id. little complicated in conference scenarios
                

                switch (data.event) {   
                    case 'connected':
                        console.log(`connected event ${message}`);
                        await session.setupPromptStart();  
                        break;
                    case 'start':                        
                        
                        await session.setupSystemPrompt(undefined, SYSTEM_PROMPT);                        
                        await session.setupStartAudio();

                        session.streamSid = data.streamSid;
                        callSid = data.start.callSid; //call sid to update while redirecting it to SIP endpoint
                        console.log(`Stream started streamSid: ${session.streamSid}, callSid: ${callSid}`);
                        
                        //send the audio bytes that say "hello" as to mimick the user greeting to allow model to speak first
                        await session.streamAudio(helloAudioBytes);
                        break;

                    case 'media':            
                        
                        if (!(session.streamSid)) break;
                        //console.log(`Audio ${data.media.track} - sequence: ${data.sequenceNumber}`);
                        //convert from 8-bit mulaw to 16-bit LPCM
                        const audioInput = Buffer.from(data.media.payload, 'base64');
                        const pcmSamples = mulaw.decode(audioInput);
                        const audioBuffer = Buffer.from(pcmSamples.buffer);

                        //send audio to nova client
                        //const audioBuffer = data.media.payload;                        
                        await session.streamAudio(audioBuffer);                     
                        break;
                    
                    default:
                        console.log('Received non-media event:', data.event);
                        break;
                }
            } catch (error) {
                console.error('Error parsing message:', error, 'Message:', message);
                connection.close();
            }
        });

        // Handle connection close
        connection.on('close', () => {
            console.log('Client disconnected.');
        });


        /**
         * Handle all the Nova Sonic events
         */

        // Set up event handlers
        session.onEvent('contentStart', (data) => {
            console.log('contentStart:', data);
            //socket.emit('contentStart', data);
        });

        session.onEvent('textOutput', (data) => {
            console.log('Text output:', data.content.substring(0, 50) + '...');
            //socket.emit('textOutput', data);
        });

        session.onEvent('audioOutput', (data) => {
            //console.log('Audio output received, sending to client');
            //socket.emit('audioOutput', data);
            //send the audio back to twilio
            //console.log('audioOutput')

            // Decode base64 to get the PCM buffer
            const buffer = Buffer.from(data['content'], 'base64');
            // Convert to Int16Array (your existing code is correct here)
            const pcmSamples = new Int16Array(buffer.buffer, buffer.byteOffset, buffer.length / Int16Array.BYTES_PER_ELEMENT);
            // Encode to mulaw (8-bit)
            const mulawSamples = mulaw.encode(pcmSamples);
            // Convert to base64
            const payload = Buffer.from(mulawSamples).toString('base64');


            const audioResponse = { 
                    event: "media", 
                    media: { 
                        track: "outbound",
                        payload
                    } ,
                    "streamSid": session.streamSid
               }

            connection.send(JSON.stringify(audioResponse));
        });

        session.onEvent('error', (data) => {
            console.error('Error in session:', data);
            //socket.emit('error', data);
            //optionally close the connection based on the error            
        });

        session.onEvent('toolUse', async (data) => {
            console.log('Tool use detected:', data.toolName);
            if (data.toolName == 'support') {      
                console.log(`Transfering call id ${callSid}`);
                try {
                    await twClient.calls(callSid).update({twiml: sipTwiml});    
                } catch (error) {
                    console.log(error);
                }          
                
            }
            //socket.emit('toolUse', data);
        });

        session.onEvent('toolResult', (data) => {
            console.log('Tool result received');
            //socket.emit('toolResult', data);
        });

        session.onEvent('contentEnd', (data) => {
            console.log('Content end received');
            if (data["stopReason"] == "INTERRUPTED") {
                //since you received more media send a clear message
                const clearMessage = {
                    "event": "clear",
                    "streamSid": session.streamSid
                }
                connection.send(JSON.stringify(clearMessage));
            }
            //socket.emit('contentEnd', data);
        });

        session.onEvent('streamComplete', () => {
            console.log('Stream completed for client:', session.streamSid);
            //socket.emit('streamComplete');            
        });



    });
});


fastify.listen({ port: 3000 }, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server is listening on port ${3000}`);
});