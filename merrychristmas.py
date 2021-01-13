import boto3

path = "/var/www/html/assets/audio/"

voices = ["Brian", "Amy"]

session = boto3.session.Session(aws_access_key_id="AKIAJ37H4XBNDTXP6GJQ",
                                aws_secret_access_key="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",
                                region_name="us-east-2")

polly = session.client('polly')

text= "I just wanted to thank you all for supporting and listening on Wallstreet Bets Synth this past year. Merry Christmas to all you rainbow bears and bulls. Cheers."

for voice in voices:
    r = polly.synthesize_speech(
        Engine = "standard",
        OutputFormat = "mp3",
        Text = text,
        VoiceId = voice
    )

    fname = "merrychristmas_"+ voice +".mp3"
    
    
    with open(path + fname, 'wb') as f:
        f.write(r['AudioStream'].read())