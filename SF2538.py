import streamlit as st
import streamlit.components.v1 as components

# The URL for your updated client site
new_url = "https://soilfreeze.streamlit.app/?job=2527"

# JavaScript to immediately redirect the browser
# We use window.parent.location to ensure it breaks out of Streamlit's iframe
redirect_code = f"""
<script>
    window.parent.location.href = '{new_url}';
</script>
"""

# Show a quick loading message just in case it takes a split second
st.write("Redirecting to the updated site...")

# Execute the redirect
components.html(redirect_code)
