import streamlit as st
import streamlit.components.v1 as components

new_url = "https://soilfreeze.streamlit.app/?job=2538"

st.write("Redirecting to the updated site...")

# The manual fallback link
st.markdown(f"**[If this doesn't update, click here.]({new_url})**")

# The automatic redirect attempt (using window.top to try and bypass the iframe)
redirect_code = f"""
<meta http-equiv="refresh" content="0; url={new_url}">
<script>
    window.top.location.href = '{new_url}';
</script>
"""

components.html(redirect_code)
