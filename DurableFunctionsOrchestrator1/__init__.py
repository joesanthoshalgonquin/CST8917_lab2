import azure.functions as func
import azure.durable_functions as df
from PIL import Image, ImageDraw, ImageFont
import io

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# A Blob Triggered Function with a Durable Functions Client binding
@myApp.blob_trigger(arg_name="myblob", path="mycontainer", connection="CONNECTION_STRING")
@myApp.durable_client_input(client_name="client")
async def http_start(blob: func.InputStream, client):
    await client.start_new("hello_orchestrator", client_input=blob)

# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    blob: func.InputStream = context.get_input()
    try:
        blob.seek(0)
        img_bytes =blob.read()
        img = Image.open(io.BytesIO(img_bytes))
        resized_image = context.call_activity("resize_image", img)
        grayscaled_image = context.call_activity("grayscale_image", resized_image)
        watermarked_image = context.call_activity("watermark_image", grayscaled_image)

        return watermarked_image

    except Exception as err:
        print(f"Error converting stream to image: {err}")

@myApp.activity_trigger(input_name="blobImage")
def resize_image(blobImage: Image.Image):
    return blobImage.resize((1024, 768))

@myApp.activity_trigger(input_name="blobImage")
def grayscale_image(blobImage: Image.Image):
    return blobImage.convert('L')

@myApp.activity_trigger(input_name="blobImage")
def watermark_image(blobImage: Image.Image):
    try:
        drawn_img = ImageDraw.Draw(blobImage)
        img_font = ImageFont.truetype("arial.ttf", 36)
        text = "Hello There"
        text_width, text_height = drawn_img.textsize(text, font=img_font)
        position = ((blobImage.width - text_width) // 2, (blobImage.height - text_height) // 2)
        drawn_img.text(position, text, fill="red", font=img_font)
        return blobImage

    except Exception as err:
        print(f"Error adding watermark: {err}")
        return blobImage
