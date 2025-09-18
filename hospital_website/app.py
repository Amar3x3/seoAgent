# app.py

from flask import Flask, render_template, request, jsonify


app = Flask(__name__)


# Add new keys for the visible page content
WEBSITE_METADATA = {
    "title": "Default Title | Apollo Hospitals",
    "description": "This is the default description.",
    "page_h1": "Best Orthopedics Hospital in Chennai",
    "page_paragraph": "The Department of Orthopaedics at Apollo Hospitals, Chennai is renowned for delivering advanced Orthopaedics care."
}

@app.route('/', methods=['GET'])
def homepage():
    """
    Renders the main page, passing all dynamic content to the template.
    """
    global WEBSITE_METADATA
    return render_template(
        'index.html',
        title=WEBSITE_METADATA['title'],
        description=WEBSITE_METADATA['description'],
        page_h1=WEBSITE_METADATA['page_h1'],
        page_paragraph=WEBSITE_METADATA['page_paragraph']
    )

@app.route('/update-metadata', methods=['POST'])
def update_metadata():
    """
    API endpoint to update all dynamic content of the website.
    """
    global WEBSITE_METADATA
    
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Missing JSON payload"}), 400
        
    # Update each piece of metadata if it's provided in the request
    WEBSITE_METADATA['title'] = data.get('title', WEBSITE_METADATA['title'])
    WEBSITE_METADATA['description'] = data.get('description', WEBSITE_METADATA['description'])
    WEBSITE_METADATA['page_h1'] = data.get('page_h1', WEBSITE_METADATA['page_h1'])
    WEBSITE_METADATA['page_paragraph'] = data.get('page_paragraph', WEBSITE_METADATA['page_paragraph'])
    
    print(f"✅ Metadata updated successfully: {WEBSITE_METADATA}")
    
    return jsonify({"status": "success", "message": "Website content updated."})

# Note: The `if __name__ == '__main__':` block is not needed for Cloud Run.