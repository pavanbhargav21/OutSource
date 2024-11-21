
from pywinauto import Desktop, Application
from pywinauto.keyboard import send_keys
from datetime import datetime
import json
import os
import time

# Define paths for JSON output and other settings
process_folder = r"C:\pulse_event_trigger\process"
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def find_window_by_title(partial_title):
    """Find a window by its partial title."""
    while True:
        try:
            windows = Desktop(backend="uia").windows()
            for win in windows:
                if partial_title in win.window_text():
                    print(f"Found window: {win.window_text()}")
                    return win
        except Exception as e:
            print(f"Error finding window: {e}")
        time.sleep(1)


def find_element_by_text(window, search_text):
    """Find an element in the window that matches the search text."""
    try:
        window.set_focus()
        elements = window.descendants(control_type="Text")  # Get all text elements
        for element in elements:
            if search_text.lower() in element.window_text().lower():
                print(f"Found element with text: {element.window_text()}")
                return element
        print(f"No element found with text: {search_text}")
    except Exception as e:
        print(f"Error finding element: {e}")
    return None


def scroll_and_interact(window, element, search_text):
    """Scroll to an element and interact with it."""
    try:
        # Ensure the window is focused
        window.set_focus()

        # Simulate scrolling until the element is visible
        for _ in range(10):  # Adjust scroll attempts as needed
            element = find_element_by_text(window, search_text)
            if element:
                # Right-click on the found element
                element.right_click_input()
                send_keys("^c")  # Simulate Ctrl+C for copying
                return True
            else:
                # Scroll down if the element is not found
                send_keys("{PGDN}")  # Page Down
                time.sleep(1)
        print("Failed to locate the element after scrolling.")
    except Exception as e:
        print(f"Error interacting with element: {e}")
    return False


def capture_summary_data(window, search_text="Summary"):
    """Capture summary data by searching for a specific element."""
    try:
        # Find the desired element by text and scroll to it
        element = find_element_by_text(window, search_text)
        if not element:
            print(f"Scrolling to find the element with text '{search_text}'")
            if not scroll_and_interact(window, element, search_text):
                return

        # Fetch the copied content
        summary_text = os.popen("powershell -command Get-Clipboard").read()
        print("Summary Text copied:", summary_text)

        # Parse the pasted content into key-value pairs
        summary_dict = {}
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
        print("Summary Dict:", summary_dict)

        # Update data_dict and save to JSON
        start_id_time = datetime.now()
        data_dict.update(summary_dict)
        data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
        save_to_json(data_dict)
        global json_created
        json_created = True
        print("JSON created with Summary data.")
    except Exception as e:
        print(f"Error capturing summary data: {e}")


def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    """Monitor a specific window and capture data."""
    global json_created
    window = find_window_by_title(target_title)
    print(f"Tracking started for '{target_title}'")
    
    start_time = datetime.now()
    data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    # Capture data from the window
    capture_summary_data(window)


# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")



from pywinauto import Desktop, Application
from pywinauto.keyboard import send_keys
from datetime import datetime
import json
import os
import time

# Define paths for JSON output and other settings
process_folder = r"C:\pulse_event_trigger\process"
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def find_window_by_title(partial_title):
    """Find a window by its partial title."""
    while True:
        try:
            windows = Desktop(backend="uia").windows()
            for win in windows:
                if partial_title in win.window_text():
                    print(f"Found window: {win.window_text()}")
                    return win
        except Exception as e:
            print(f"Error finding window: {e}")
        time.sleep(1)


def get_window_coordinates(window):
    """Retrieve the coordinates of the given window."""
    rect = window.rectangle()
    coords = {
        "left": rect.left,
        "top": rect.top,
        "right": rect.right,
        "bottom": rect.bottom,
        "width": rect.width(),
        "height": rect.height(),
    }
    print(f"Window coordinates: {coords}")
    return coords


def capture_summary_data(window):
    """Capture summary data by simulating interactions."""
    try:
        # Right-click on the window and copy data
        window.set_focus()
        window.right_click_input()
        send_keys("^c")  # Simulate Ctrl+C for copying
        
        # Fetch the copied content
        summary_text = os.popen("powershell -command Get-Clipboard").read()
        print("Summary Text copied:", summary_text)

        # Parse the pasted content into key-value pairs
        summary_dict = {}
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
        print("Summary Dict:", summary_dict)

        # Update data_dict and save to JSON
        start_id_time = datetime.now()
        data_dict.update(summary_dict)
        data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
        save_to_json(data_dict)
        global json_created
        json_created = True
        print("JSON created with Summary data.")
    except Exception as e:
        print(f"Error capturing summary data: {e}")


def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    """Monitor a specific window and capture data."""
    global json_created
    window = find_window_by_title(target_title)
    print(f"Tracking started for '{target_title}'")
    
    start_time = datetime.now()
    data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    # Get window coordinates (if needed)
    coords = get_window_coordinates(window)

    # Capture data from the window
    capture_summary_data(window)


# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")





Overview of PyAutoGUI

PyAutoGUI is a Python library that automates interactions with the GUI. It allows developers to simulate mouse movements, keyboard presses, and screen interactions, including locating images or regions on the screen. It works by capturing screenshots of the screen and searching for visual matches.


---

Pros of PyAutoGUI in Your Situation

1. Ease of Use

PyAutoGUI has simple and intuitive functions (locateOnScreen, click, drag, etc.) for screen automation and locating images.



2. Cross-Platform Support

Works on Windows, macOS, and Linux, making it versatile for different environments.



3. Built-in Screenshot and Image Matching

PyAutoGUI directly integrates image recognition with locateOnScreen, making it ideal for scenarios where you need to identify elements visually.



4. Confidence Matching

Supports a confidence parameter for image matching (e.g., confidence=0.8), making it flexible for images with slight variations.



5. Good for On-Screen Matching

PyAutoGUI performs well when matching smaller images (e.g., partial image) on the screen directly, which is suitable for GUI-based tasks.



6. No Manual Coordinates

You don't need to manually define coordinates; it dynamically finds the image's location, which simplifies automation tasks.





---

Limitations of PyAutoGUI

1. Dependency on Screen Capture

PyAutoGUI relies on screen resolution and the pixel-perfect matching of images. If the captured image (e.g., via MSS) differs in scale, DPI, or compression, it may fail to match.



2. Performance

Matching large images or searching within high-resolution screens can be slow, as it scans pixel by pixel.



3. Limited to Static Images

It cannot handle dynamic or real-time visual changes effectively. For example, partial transparency or animations may cause matching to fail.



4. Accuracy Issues

Matching depends on exact pixel values. Even small variations in color, resolution, or scale can result in failure, especially when the source images are captured using different tools (e.g., MSS vs. Snipping Tool).



5. Lack of Advanced Image Processing

Unlike OpenCV, PyAutoGUI doesn’t support advanced image preprocessing (e.g., scaling, rotation, or feature-based matching), limiting its flexibility in handling mismatches.



6. Screen-Specific

PyAutoGUI is designed to work on live screens. Matching images within saved files or screenshots is less efficient and not its primary purpose.



7. Platform-Specific Constraints

On certain platforms or with multi-monitor setups, PyAutoGUI may face challenges capturing or processing images correctly.





---

When to Use PyAutoGUI in Your Scenario

Best Use Case:
If you're matching partial images directly on the live screen, PyAutoGUI is an excellent choice because it’s built for GUI-based automation tasks.

Not Recommended For:
If your process requires matching within static screenshots or handling images captured via different tools (e.g., MSS vs. Snipping Tool). In such cases, OpenCV or feature-based methods are more reliable.



---

How to Mitigate PyAutoGUI’s Limitations

1. Preprocess Images:
Save screenshots using the same tool (e.g., MSS) and ensure consistent resolution and DPI.


2. Adjust Confidence Threshold:
Use a lower confidence threshold (e.g., confidence=0.7) to account for minor differences in images.


3. Scale Matching:
Ensure the partial image’s scale matches the full image exactly by resizing if needed.


4. Switch to OpenCV or MSS for Advanced Matching:
If PyAutoGUI consistently fails, use OpenCV for precise and advanced matching, especially for static screenshots or secondary monitor scenarios.




---

By understanding PyAutoGUI's strengths and limitations, you can decide whether to stick with it for live screen-based tasks or opt for alternatives like OpenCV for more complex image processing requirements.

















def process_key_store(self, key_store_data, workflow_dict, keyname_store_set, keyname_mapping_set,
                      business_function_dict, delivery_function_dict, process_function_dict,
                      session, user_id, user_name, user_email):
    key_store_entries = key_store_data.to_dict(orient='records')
    key_store_request_id = None
    seen_keynames = set()
    serial_number = 1

    # Group entries by ProcessName, DeliveryService, BusinessLevel, and WorkflowName
    grouped_entries = {}
    for entry in key_store_entries:
        group_key = (
            entry['ProcessName'],
            entry['DeliveryService'],
            entry['BusinessLevel'],
            entry['WorkflowName']
        )
        grouped_entries.setdefault(group_key, []).append(entry)

    # Validate each group
    for group_key, entries in grouped_entries.items():
        is_unique_values = [entry['UniqueKey'] for entry in entries]
        yes_count = is_unique_values.count("Yes")
        no_count = is_unique_values.count("No")

        # Check if the group has exactly one "Yes" and not all "No"
        if yes_count != 1:
            return jsonify({
                'message': f'Group {group_key} must have exactly one "Yes" in UniqueKey field.'
            }), 400
        if no_count == len(is_unique_values):
            return jsonify({
                'message': f'Group {group_key} cannot have all entries as "No" in UniqueKey field.'
            }), 400

    # Process each entry after validation
    for entry in key_store_entries:
        workflow_name = entry['WorkflowName']
        workflow_id = workflow_dict.get(workflow_name)

        if workflow_id is None:
            return jsonify({'message': f'Workflow "{workflow_name}" does not exist in KEY_STORE sheet'}), 400

        key_name = entry['KeyName']
        if key_name in seen_keynames:
            return jsonify({'message': f'Duplicate KeyName "{key_name}" in KEY_STORE sheet'}), 400

        seen_keynames.add(key_name)

        if (workflow_id, key_name) in keyname_store_set or (workflow_id, key_name) in keyname_mapping_set:
            return jsonify({'message': f'Duplicate KeyName "{key_name}" for Workflow "{workflow_name}" in KEY_STORE'}), 400

        if not key_store_request_id:
            new_request = KeynameStoreRequests(
                count=len(key_store_entries),
                req_created_date=datetime.utcnow(),
                modified_date=datetime.utcnow(),
                created_by=user_id,
                creator_name=user_name,
                creator_email=user_email,
                is_active=True,
                status="open",
            )
            session.add(new_request)
            session.flush()
            key_store_request_id = new_request.request_id

        new_keyname_config = KeynameStoreConfigRequests(
            request_id=key_store_request_id,
            workflow_id=workflow_id,
            serial_number=serial_number,
            business_level_id=business_function_dict.get(entry['BusinessLevel']),
            delivery_service_id=delivery_function_dict.get(entry['DeliveryService']),
            process_name_id=process_function_dict.get(entry['ProcessName']),
            activity_key_name=key_name,
            activity_key_layout=entry['Layout'],
            is_unique=entry['UniqueKey'] == 'Yes',
            remarks=str(entry['Remarks']),
            is_active=True,
            status_ar='open'
        )
        session.add(new_keyname_config)
        serial_number += 1

    return None




import cv2
import pyautogui
from screeninfo import get_monitors

def get_screen_resolution():
    """
    Get the resolution of the primary screen using ScreenInfo.
    """
    for monitor in get_monitors():
        # Assuming the primary monitor
        print(f"Monitor: {monitor.name}, Resolution: {monitor.width}x{monitor.height}")
        return monitor.width, monitor.height

def perform_image_matching(fixed_image_path, fixed_resolution, threshold=0.8):
    """
    Perform image matching by scaling the fixed image to match the current screen resolution.
    
    Args:
        fixed_image_path: Path to the fixed image.
        fixed_resolution: Tuple of (width, height) for the fixed image's original resolution.
        threshold: Confidence threshold for template matching.

    Returns:
        The coordinates of the top-left corner of the matched area if found, else None.
    """
    # Get the current screen resolution
    current_width, current_height = get_screen_resolution()
    print(f"Current Screen Resolution: {current_width}x{current_height}")

    # Scaling factors
    original_width, original_height = fixed_resolution
    scale_width = current_width / original_width
    scale_height = current_height / original_height

    # Capture the current screen
    screenshot = pyautogui.screenshot()
    screenshot.save("screenshot.png")

    # Load images
    fixed_image = cv2.imread(fixed_image_path)
    screenshot_image = cv2.imread("screenshot.png")

    # Resize the fixed image to match the current resolution
    resized_fixed_image = cv2.resize(fixed_image, (0, 0), fx=scale_width, fy=scale_height)

    # Perform template matching
    result = cv2.matchTemplate(screenshot_image, resized_fixed_image, cv2.TM_CCOEFF_NORMED)

    # Get the best match position
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)

    print(f"Match confidence: {max_val}")
    if max_val >= threshold:
        print(f"Match found at coordinates: {max_loc}")
        # Highlight the matched area on the screenshot (optional)
        top_left = max_loc
        h, w = resized_fixed_image.shape[:2]
        bottom_right = (top_left[0] + w, top_left[1] + h)
        cv2.rectangle(screenshot_image, top_left, bottom_right, (0, 255, 0), 2)
        cv2.imwrite("output_match.png", screenshot_image)
        return top_left
    else:
        print("No match found.")
        return None

if __name__ == "__main__":
    # Path to the fixed image (1920x1080 resolution)
    fixed_image_path = "fixed_image.png"

    # Fixed resolution of the reference image
    fixed_resolution = (1920, 1080)

    # Perform the image matching
    match_coordinates = perform_image_matching(fixed_image_path, fixed_resolution)

    if match_coordinates:
        print(f"Element located at: {match_coordinates}")
    else:
        print("Element not found on the screen.")




import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os
from pynput.mouse import Listener

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info:", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles:", windows)
        
        for win in windows:
            if target_title in win:
                print("Checking with", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window is:", window)
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor['x'],
            "top": monitor['y'],
            "right": monitor['x'] + monitor['width'],
            "bottom": monitor['y'] + monitor['height']
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect['left'], monitor_rect['left'])
        overlap_top = max(window_rect['top'], monitor_rect['top'])
        overlap_right = min(window_rect['right'], monitor_rect['right'])
        overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")
        
        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None

def capture_screenshot(monitor_index, monitor_details):
    """Capture a screenshot of a specific monitor."""
    monitor = monitor_details[monitor_index]
    with mss.mss() as sct:
        screenshot = sct.grab({
            "top": monitor["y"],
            "left": monitor["x"],
            "width": monitor["width"],
            "height": monitor["height"]
        })
    return screenshot

def track_image(image_path, monitor_index, monitor_details):
    """Locate an image on a specific monitor."""
    screenshot = capture_screenshot(monitor_index, monitor_details)
    screenshot_path = os.path.join(process_folder, f"temp_monitor_{monitor_index}.png")
    mss.tools.to_png(screenshot.rgb, screenshot.size, output=screenshot_path)

    # Locate the image using PyAutoGUI
    location = pyautogui.locateOnScreen(image_path, confidence=0.9, region=(
        monitor_details[monitor_index]["x"],
        monitor_details[monitor_index]["y"],
        monitor_details[monitor_index]["width"],
        monitor_details[monitor_index]["height"]
    ))

    if location:
        x, y = pyautogui.center(location)
        print(f"Image {image_path} found at {x}, {y} on monitor {monitor_index}")
        return (x, y)
    else:
        print(f"Image {image_path} not found on monitor {monitor_index}")
        return None

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
    print("Summary Dict:", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict)
    global json_created
    json_created = True
    print("JSON created with Summary data.")

def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            location = track_image(summary_button_path, monitor_index, monitor_details)
            if location:
                capture_summary_data(location, start_time)
            time.sleep(1)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")





def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor['x'],
            "top": monitor['y'],
            "right": monitor['x'] + monitor['width'],
            "bottom": monitor['y'] + monitor['height']
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect['left'], monitor_rect['left'])
        overlap_top = max(window_rect['top'], monitor_rect['top'])
        overlap_right = min(window_rect['right'], monitor_rect['right'])
        overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")
        
        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None


----






import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os
from pynput.mouse import Listener

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info ", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles",windows)
        
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the window is open."""    
    # Loop through each monitor and find where the window is located
    for index, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
                monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index

    # If no monitor contains the window, default to the first monitor
    print("Window position did not match any monitor, defaulting to primary monitor.")
    return 0 if monitors else None

def track_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    else:
        return None, None

def monitor_clicks_and_track_end_button(end_button_path):
    """Monitor clicks and track the 'end button' when clicked."""
    print("Monitoring clicks to find the 'end button'.")
    end_button_location = None
    while True:
        # Start listening for a mouse click event
        with Listener(on_click=on_click) as listener:
            listener.join()  # Wait for click event to trigger

            # Once clicked, check for the end button location
            location_end, end_time = track_final_image(end_button_path, "end_button")
            if location_end:
                # Store end_button location details
                end_button_region = {
                    "left": location_end[0] - 10,
                    "top": location_end[1] - 10,
                    "right": location_end[2] + 10,
                    "bottom": location_end[3] + 10
                }
                print("End button detected, monitoring for clicks in region.")
                
                # Monitor clicks in the end button region
                monitor_clicks_in_region(end_button_region)
                break  # Exit after tracking the 'end button' event

        time.sleep(0.1)  # Allow for quick reaction to clicks

def track_final_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    print("Is location ?", location )
    if location:
        w, x, y, z = location.left, location.top, location.width, location.height 
        print(f"{action_name} located at {w}, {x}, {y}, {z}")
        return (w, x, y, z), datetime.now()
    else:
        return None, None

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value
            else:
                case_id = "dummy"
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")


def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            time.sleep(5)
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            # Track summary_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    # JSON creation and copying data once
                    capture_summary_data(location, start_id_time)


            # Start monitoring for end button after JSON creation
            if json_created:
                monitor_clicks_and_track_end_button(end_button_path)
                return  # Exit after tracking the 'end button' event
            
            time.sleep(1)  # Check every second


def monitor_clicks_in_region(region):
    """Monitor for clicks in a specific region and take a screenshot."""
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                
                # Capture full screen and save to process folder
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break  # Exit after capturing the screenshot
            time.sleep(0.1)  # Check clicks frequently

def on_click(x, y, button, pressed):
    """Callback function for mouse click events."""
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        return False  # Stop listener after a click (you can adjust behavior here)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"

    # Run the monitor process in a separate thread
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()



def monitor_clicks_and_track_end_button(end_button_path):
    """Monitor clicks and track the 'end button' when clicked."""
    print("Monitoring clicks to find the 'end button'.")
    while True:
        x, y = pyautogui.position()
        pyautogui.waitForClick()  # Wait for a click event
        
        # After a click, try to locate the 'end button'
        location_end, end_time = track_final_image(end_button_path, "end_button")
        if location_end:
            # Store end_button location details
            end_button_region = {
                "left": location_end[0] - 10,
                "top": location_end[1] - 10,
                "right": location_end[2] + 10,
                "bottom": location_end[3] + 10
            }
            print("End button detected, monitoring for clicks in region.")
            
            # Monitor clicks in the end button region
            monitor_clicks_in_region(end_button_region)
            break  # Exit after tracking the 'end button' event
        
        time.sleep(0.1)  # Allow for quick reaction to clicks

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
            # Track summary_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    # Capture and process summary data
                    capture_summary_data(location, start_id_time)
            
            # Start monitoring for end button after JSON creation
            if json_created:
                monitor_clicks_and_track_end_button(end_button_path)
                return  # Exit after tracking the 'end button' event
            
            time.sleep(1)  # Check every second

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value
            else:
                case_id = "dummy"
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")





import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    while True:
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    for index, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
                monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index
    return 0 if monitors else None

def track_image(image_path, action_name):
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    return None, None

def track_final_image(image_path, action_name):
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        w, x, y, z = location.left, location.top, location.width, location.height
        print(f"{action_name} located at {w}, {x}, {y}, {z}")
        return (w, x, y, z), datetime.now()
    return None, None

def save_to_json(data, case_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    x, y = location
                    curr_x, curr_y = pyautogui.position()
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.moveTo(x + 30, y + 30)
                    pyautogui.click()
                    summary_text = pyperclip.paste()
                    pyautogui.moveTo(curr_x, curr_y)
                    summary_dict = {}
                    lines = summary_text.split('\n')
                    for line in lines:
                        if line.strip():
                            parts = line.split('\t')
                            key = parts[0].strip() if len(parts) > 0 else ""
                            value = parts[1].strip() if len(parts) > 1 else ""
                            summary_dict[key] = value
                            if key == "Id":
                                case_id = value
                            else:
                                case_id = "dummy"
                    print("Summary Dict", summary_dict)
                    data_dict.update(summary_dict)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, case_id)
                    json_created = True
                    print("JSON created with Summary data.")

            if json_created:
                while True:
                    location_end, end_time = track_final_image(end_button_path, "end_button")
                    if location_end:
                        end_button_region = {
                            "left": location_end[0] - 10,
                            "top": location_end[1] - 10,
                            "right": location_end[2] + 10,
                            "bottom": location_end[3] + 10
                        }
                        print("End button detected, monitoring for clicks in region.")
                        monitor_clicks_in_region(end_button_region)
                        return
                    time.sleep(1)

            time.sleep(1)

def monitor_clicks_in_region(region):
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break
            time.sleep(0.1)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()




import screeninfo
import pygetwindow as gw

def get_monitor_index(window_title):
    # Get a list of all monitors
    monitors = screeninfo.get_monitors()
    # Try to find the specified window by title
    try:
        window = gw.getWindowsWithTitle(window_title)[0]
    except IndexError:
        print(f"Window with title '{window_title}' not found.")
        return None

    # Get the window's x, y position
    win_x, win_y = window.left, window.top
    
    # Loop through each monitor and find where the window is located
    for index, monitor in enumerate(monitors):
        if (monitor.x <= win_x < monitor.x + monitor.width and
                monitor.y <= win_y < monitor.y + monitor.height):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index

    # If no monitor contains the window, default to the first monitor
    print("Window position did not match any monitor, defaulting to primary monitor.")
    return 0 if monitors else None

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # Get X, Y from located position of the okay_button
                    x, y = location
                    
                    # Move to detected location and adjust to the appropriate offset for right-click
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()  # Right-click on the okay_button location
                    time.sleep(0.5)

                    # Move to "Copy" option in the context menu and click to copy text
                    pyautogui.moveTo(x + 40, y + 60)  # Adjust coordinates to hover on "Copy"
                    pyautogui.click()  # Click on "Copy" option
                    time.sleep(0.5)

                    # Paste the copied content
                    summary_text = pyperclip.paste()

                    # Parse the pasted content into key-value pairs
                    summary_dict = {}
                    lines = summary_text.split('\n')
                    for line in lines:
                        if line.strip():  # Ignore empty lines
                            parts = line.split('\t')  # Split by tab
                            key = parts[0].strip() if len(parts) > 0 else ""
                            value = parts[1].strip() if len(parts) > 1 else ""
                            summary_dict[key] = value
                    
                    # Update data_dict with parsed summary_dict
                    data_dict.update(summary_dict)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second




Given the updated requirements, here’s an approach to handle each step:

1. Detect the OK Button: When the okay_button image is first found, create a JSON file named with the specified format, and include details such as timestamp and extracted text. Skip creating the JSON file again if it already exists.


2. Monitor for the End Button: Once the okay_button is detected and JSON is created, start monitoring for end_button.


3. Handle Clicks in the End Button Region: If end_button is detected, track its coordinates (left, top, bottom, width). If any clicks happen within this region, immediately take a full screenshot and store it in the process folder.


4. Threading and Click Monitoring: Use threads for continuous monitoring and to ensure click events in the end_button region are detected for capturing screenshots.



Here’s how the updated code would look:

import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os

# Define paths for images and folders
okay_button_path = "path/to/okay_button.png"
end_button_path = "path/to/end_button.png"
process_folder = "path/to/process_folder"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the window is open."""
    for i, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
            monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            return i
    return None

def track_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    else:
        return None, None

def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # JSON creation and copying data once
                    x, y = location
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.hotkey('ctrl', 'c')
                    copied_text = pyperclip.paste()  # Copy extracted data

                    # Store data in dictionary
                    data_dict['copied_data'] = copied_text
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second

def monitor_clicks_in_region(region):
    """Monitor for clicks in a specific region and take a screenshot."""
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                
                # Capture full screen and save to process folder
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break  # Exit after capturing the screenshot
            time.sleep(0.1)  # Check clicks frequently

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"

    # Run the monitor process in a separate thread
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()

Explanation of Updates:

1. Flag for JSON Creation (json_created):

A flag json_created is set to False initially.

Once the okay_button is detected and JSON is created, json_created is set to True, so subsequent detections of okay_button will be ignored.



2. monitor_clicks_in_region Function:

Tracks clicks within the end_button region.

On detecting a click in the region, a full screenshot is taken and saved in the process_folder with a timestamped name.

After taking a screenshot, the function breaks out, ending the monitoring.



3. Modifications to monitor_process:

JSON creation is now triggered only on the first detection of okay_button.

Once the JSON is created, end_button tracking begins.

When end_button is detected, the system monitors clicks within its region and captures a screenshot on a detected click.




This code structure meets your requirements for efficient, click-based tracking of the end_button and conditions around JSON creation. Adjust file paths, image paths, and case_id as necessary for your specific setup.


____((((()))))___&&

Thank you for the clarification! Here’s how we can update the code to parse the copied text line-by-line, split it into key-value pairs, and store it as separate entries in the dictionary. This way, each line will be processed individually, with the first part as the key and the remaining part as the value.

Here's the updated monitor_process function with the necessary changes:

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # JSON creation and copying data once
                    x, y = location
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.hotkey('ctrl', 'c')
                    copied_text = pyperclip.paste()  # Copy extracted data

                    # Parse the copied text line by line into key-value pairs
                    parsed_data = {}
                    for line in copied_text.splitlines():
                        if line.strip():  # Ignore empty lines
                            key_value = line.split(maxsplit=1)  # Split into key and value based on first whitespace
                            if len(key_value) == 2:  # Ensure there is both a key and a value
                                key, value = key_value
                                parsed_data[key.strip()] = value.strip()

                    # Store parsed key-value pairs in data_dict
                    data_dict.update(parsed_data)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second

Explanation of Updates:

1. Parsing copied_text into Key-Value Pairs:

Each line of copied_text is processed individually by splitting it into key and value using line.split(maxsplit=1). This splits only at the first space, ensuring the key and value are separated correctly.

Only lines that contain both a key and a value are included in parsed_data.

Each key and value is stripped of extra whitespace for cleaner formatting.



2. Updating data_dict with Parsed Data:

After parsing, data_dict.update(parsed_data) is used to add the key-value pairs into data_dict.

This ensures that each key-value pair from the copied text is stored individually in the JSON.



3. Creating JSON with Parsed Data:

save_to_json(data_dict, case_id) is called to save the formatted data in JSON format, as previously.




This will result in a JSON file where each extracted line is stored as a separate key-value pair, as specified.




-----------------
import pyautogui
import time

# Move the mouse to the top-left corner of the first monitor (laptop screen)
pyautogui.moveTo(100, 100)
print("Moved mouse to first monitor")

# Wait for 2 seconds to observe the mouse
time.sleep(2)

# Move the mouse to the top-left corner of the second monitor (TV)
# Adjust the coordinates based on your setup (e.g., if your TV is to the right, x=1920 for 1080p laptop)
pyautogui.moveTo(1920 + 100, 100)
print("Moved mouse to second monitor (TV)")

# Wait for 2 seconds to observe the mouse
time.sleep(2)

# Perform a click action on the second monitor
pyautogui.click(1920 + 100, 100)  # Adjust based on the screen resolution and location
print("Clicked on second monitor")






To handle exiting the mouse.Listener loop based on a flag, you can use a global variable that gets updated when the flag condition is met. This flag can be checked inside the on_click function, and if the condition is true, it will stop the listener.

To implement this, modify the on_click function to check the flag after each click. If the flag indicates that the program should stop, you can call listener.stop() to exit the loop.

Here's how you can modify the code:

import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "test_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click and the button region
awaiting_final_ok_click = False
ok_button_region = None  # Store button's bounding box
exit_flag = False  # Flag to indicate when to stop the listener

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    global ok_button_region
    try:
        print("Checking for button visibility...")
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.9)
        if location:
            print(f"Button found at {location.left}, {location.top}, {location.width}, {location.height}")
            # Store the bounding box as (left, top, width, height)
            ok_button_region = location
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click, ok_button_region, exit_flag

    # Only proceed on button release
    if not pressed:
        print(f"Mouse clicked at ({x}, {y})")
        if awaiting_final_ok_click:
            # Check if the OK button is visible and a region is defined
            if ok_button_region:
                # Check if click is within the button region
                left, top, width, height = ok_button_region.left, ok_button_region.top, ok_button_region.width, ok_button_region.height
                if left <= x <= left + width and top <= y <= top + height:
                    print("Click is within OK button region. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
                    exit_flag = True  # Set exit flag to stop the listener
        else:
            # Update the OK button location and enable the awaiting click state
            if check_button_visible("ok"):
                awaiting_final_ok_click = True

        # Stop the listener if the exit flag is set
        if exit_flag:
            return False

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")

Explanation of Changes:

1. exit_flag Variable: This variable is set to True when the condition to stop the listener is met.


2. Condition to Stop Listener: In the on_click function, if exit_flag is True, the listener stops by returning False, which breaks out of the listener.join() loop.


3. Flag Update: After capturing a screenshot, exit_flag is set to True, stopping the listener after the next click.



Now, the code will automatically stop the listener when the exit_flag is set to True, allowing you to exit the loop when the desired condition is met.






Here is a modified version of your code that incorporates the functionality you've described:

Locate Image: When a user clicks, the program will locate the image on the screen.

Move the Mouse Below the Found Button: It will move slightly below the image (button) found.

Right-Click: The program will perform a right-click.

Copy: After right-clicking, the program will copy the content.

Store the Summary in JSON: It will store the summary in a JSON file with the timestamp when the image was found.

Return to Previous Position: After completing the actions, it will return the cursor to its original position.


Modified Code:

import time
import os
import pyautogui
import pyperclip
import json
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"
SUMMARY_JSON_PATH = r"C:\pulse_event_trigger\summary_data.json"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "test_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

# Function to capture screenshot
def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

# Function to check if a button is visible and take an action
def check_button_visible(button_name, curr_x, curr_y):
    try:
        print("Searching for button...")
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.9)
        
        if location:
            print(f"Button found at location: {location.left}, {location.top}, {location.width}, {location.height}")
            
            # Capture screenshot of the area
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_check_{timestamp}.png")
            pyautogui.screenshot(screenshot_filename, region=(location.left, location.top, location.width, location.height))
            print(f"Button screenshot captured and saved as {screenshot_filename}")
            
            # Move the cursor below the button and perform right click
            pyautogui.moveTo(location.left + location.width // 2, location.top + location.height + 5)
            pyautogui.rightClick()
            print("Right-click done")

            # Wait for a moment and then perform the copy action
            time.sleep(0.5)
            pyautogui.hotkey('ctrl', 'c')
            print("Content copied")

            # Get the copied text from clipboard
            summary_text = pyperclip.paste()
            print("Summary Text:", summary_text)

            # Create JSON data for storing summary
            summary_dict = {}
            lines = summary_text.split('\n')
            for line in lines:
                parts = line.split('\t')
                if len(parts) == 2:
                    key = parts[0]
                    value = parts[1].replace('\r', '')  # Clean up the value if needed
                    summary_dict[key] = value

            # Add timestamp to the summary data
            summary_dict["timestamp"] = timestamp

            # Save to a JSON file
            if os.path.exists(SUMMARY_JSON_PATH):
                with open(SUMMARY_JSON_PATH, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = {}

            # Append new summary data
            existing_data[timestamp] = summary_dict

            with open(SUMMARY_JSON_PATH, 'w') as f:
                json.dump(existing_data, f, indent=4)

            print(f"Summary saved to {SUMMARY_JSON_PATH}")
            
            return True

        else:
            print(f"Button '{button_name}' not found on the screen.")
            return False

    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return False

# Function to track mouse clicks
def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    print(f"Mouse clicked at ({x}, {y}) with {button}")

    # Only proceed on button release
    if not pressed:
        # If we're in the final "OK" step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok", x, y)
            if ok_button_location:
                # After the button is clicked and actions are completed, reset the state
                awaiting_final_ok_click = False
        else:
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring mouse clicks
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")

Key Changes:

1. check_button_visible Function:

This function now:

Locates the image on the screen.

Takes a screenshot of the image location.

Moves the mouse slightly below the found image (button) and performs a right-click.

Copies the content after the right-click and stores it in the clipboard.

Parses the copied content into a dictionary and saves it into a JSON file with a timestamp.




2. JSON Handling:

If the JSON file already exists, it loads the existing data and appends the new summary with the timestamp.

If it doesn't exist, a new file is created.



3. Timestamp in JSON:

The timestamp is added to each summary dictionary to track when the summary was captured.



4. Return to Previous Position:

You could modify this behavior if needed to return the cursor to the original position by saving the current position before the action and restoring it afterward.




Functionality Flow:

1. Locate Image: When the user clicks on the screen, the image is located and processed.


2. Perform Right-Click: If the image is found, the mouse is moved below the image and a right-click is performed.


3. Copy and Parse Content: The content copied to the clipboard is parsed and saved in a JSON file along with the timestamp.


4. Return to Position: The cursor can be returned to the original position if needed, and the summary data is saved for later use.



This approach ensures that the user can continue working without any noticeable delay, as the operations happen in milliseconds. The timestamp will also help you track when the summary was captured.




--------------------

import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "ok_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    try:
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    # Only proceed on button release
    if not pressed:
        # If we're in the final OK step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location:
                if ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width and \
                   ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                    print("OK button clicked. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
        else:
            # Placeholder for additional logic to handle other buttons
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")






import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "ok_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    try:
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    # Only proceed on button release
    if not pressed:
        # If we're in the final OK step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location:
                if ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width and \
                   ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                    print("OK button clicked. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
        else:
            # Placeholder for additional logic to handle other buttons
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
with mouse.Listener(on_click=on_click) as listener:
    print("Monitoring started...")
    listener.join()




Your code is well-structured, but there are a few areas that could be refined to ensure smooth functioning, especially around detecting the "OK" button and handling errors. I’ve adjusted a few parts to enhance reliability and address the GetIsInvokedPattern() method, which isn’t directly available in uiautomation. I also refined the find_ok_button method to streamline the button search process and logging.

Here’s the modified version of your code:

import uiautomation as auto
import win32gui
import win32con
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "c:\\Pulserimage"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window title matches Claude's window"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}\\claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find the OK button by name within the Claude window"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Search for button by exact match on button name
            button = window.ButtonControl(Name=self.button_name)
            if button.Exists(0, 0):
                return button
            
            # Alternative search if exact match not found
            for control in window.GetChildren():
                if isinstance(control, auto.ButtonControl) and \
                   self.button_name.lower() in control.Name.lower():
                    return control
            
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_click_time = 0
        click_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists(0, 0):
                            try:
                                # Check if button was clicked
                                current_time = time.time()
                                
                                if current_time - last_click_time > click_cooldown:
                                    logging.info("OK button detected - Capturing screenshot")
                                    self.capture_screenshot(active_window)
                                    last_click_time = current_time
                            
                            except Exception as e:
                                logging.error(f"Button click detection error: {e}")
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")

Explanation of Modifications

1. Simplified find_ok_button:

Now it first tries a direct search for a button with the name self.button_name. If it doesn’t find an exact match, it searches through children controls.



2. Removed GetIsInvokedPattern:

As this is not directly available or necessary here, we rely on Exists() to check if the button is present, and we capture the screenshot if it’s detected within the Claude window.



3. Added Error Handling and Logging:

Improved logging for button detection and screenshot functions to help diagnose issues during execution.




Required Packages

Pillow: for ImageGrab, installed with pip install Pillow.

pywin32: for win32gui and win32con, installed with pip install pywin32.

uiautomation: for UI control handling, installed with pip install uiautomation.


This code will continuously monitor for the "OK" button in the specified window, and if it appears and is active, it will take a screenshot and save it with a timestamp. This way, it should meet the requirements without unnecessary complexity.





import uiautomation as auto
import win32gui
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "claude_screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        # Store last button state
        self.last_button_state = None
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window is Claude"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find OK button using multiple methods"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Method 1: Direct search
            try:
                button = window.ButtonControl(Name=self.button_name)
                if button.Exists():
                    return button
            except:
                pass
            
            # Method 2: Search all descendants
            try:
                buttons = window.GetChildren()
                for control in buttons:
                    try:
                        if isinstance(control, auto.ButtonControl) and \
                           self.button_name.lower() in control.Name.lower():
                            return control
                    except:
                        continue
            except:
                pass
            
            # Method 3: Deep search
            try:
                buttons = window.FindAll(auto.ButtonControl)
                for button in buttons:
                    try:
                        if self.button_name.lower() in button.Name.lower():
                            return button
                    except:
                        continue
            except:
                pass
                
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def is_button_clicked(self, button):
        """Check if button was clicked using multiple methods"""
        try:
            # Method 1: Check invoke pattern
            try:
                invoke_pattern = button.GetInvokePattern()
                if invoke_pattern:
                    current_state = button.CurrentIsInvoked
                    if current_state != self.last_button_state:
                        self.last_button_state = current_state
                        return current_state
            except:
                pass

            # Method 2: Check selection pattern
            try:
                selection_pattern = button.GetSelectionPattern()
                if selection_pattern:
                    return selection_pattern.IsSelected
            except:
                pass

            # Method 3: Check toggle pattern
            try:
                toggle_pattern = button.GetTogglePattern()
                if toggle_pattern:
                    return toggle_pattern.ToggleState == auto.ToggleState.On
            except:
                pass

            # Method 4: Check button state
            try:
                current_state = button.GetLegacyIAccessiblePattern().CurrentState
                was_clicked = bool(current_state & auto.State.Pressed)
                return was_clicked
            except:
                pass

            return False

        except Exception as e:
            logging.error(f"Button click check error: {e}")
            return False

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_capture_time = 0
        capture_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists():
                            # Check if button was clicked
                            current_time = time.time()
                            
                            if self.is_button_clicked(ok_button):
                                # Prevent duplicate captures
                                if current_time - last_capture_time > capture_cooldown:
                                    print("OK button clicked - Capturing screenshot")
                                    logging.info("OK button clicked - Capturing screenshot")
                                    self.capture_screenshot(active_window)
                                    last_capture_time = current_time
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")





import uiautomation as auto
import win32gui
import win32con
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "claude_screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window is Claude"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find OK button using multiple methods"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Method 1: Direct search for OK button
            try:
                button = window.ButtonControl(Name=self.button_name)
                if button.Exists():
                    return button
            except:
                pass
            
            # Method 2: Search all buttons
            try:
                all_buttons = window.GetChildren()
                for control in all_buttons:
                    try:
                        if isinstance(control, auto.ButtonControl) and \
                           self.button_name.lower() in control.Name.lower():
                            return control
                    except:
                        continue
            except:
                pass
            
            # Method 3: Deep search
            try:
                buttons = window.FindAllControl(auto.ButtonControl)
                for button in buttons:
                    try:
                        if self.button_name.lower() in button.Name.lower():
                            return button
                    except:
                        continue
            except:
                pass
                
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_click_time = 0
        click_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists():
                            try:
                                # Check if button was clicked
                                current_time = time.time()
                                
                                if ok_button.GetIsInvokedPattern():
                                    # Prevent duplicate captures
                                    if current_time - last_click_time > click_cooldown:
                                        logging.info("OK button clicked - Capturing screenshot")
                                        self.capture_screenshot(active_window)
                                        last_click_time = current_time
                            except:
                                pass
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")




-------------------

import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}

    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def find_buttons_in_window(self, window_control):
        """
        Recursively find all buttons in a window
        """
        buttons = []
        try:
            # Check if current control is a button
            if isinstance(window_control, auto.ButtonControl):
                buttons.append(window_control)
            
            # Get all children of the current control
            children = window_control.GetChildren()
            for child in children:
                # Recursively search for buttons in children
                buttons.extend(self.find_buttons_in_window(child))
                
        except Exception as e:
            logging.error(f"Error finding buttons: {e}")
        
        return buttons

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            # Get the window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                try:
                    button = window.ButtonControl(AutomationId=button_config['automation_id'])
                    if button.Exists():
                        return button
                except:
                    pass
            
            # 2. Try by Name and Class
            if button_config['class_name']:
                try:
                    button = window.ButtonControl(
                        Name=button_config['button_name'],
                        ClassName=button_config['class_name']
                    )
                    if button.Exists():
                        return button
                except:
                    pass
            
            # 3. Search all buttons and match by name
            all_buttons = self.find_buttons_in_window(window)
            for btn in all_buttons:
                try:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        return btn
                except:
                    continue
            
            return None
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button:
                            try:
                                # Check if button is clickable and was clicked
                                if button.IsEnabled:
                                    # You might need to adjust this condition based on your specific applications
                                    # Some applications might require different methods to detect if a button was clicked
                                    button_state = button.GetTogglePattern() if hasattr(button, 'GetTogglePattern') else None
                                    if button_state and button_state.ToggleState == auto.ToggleState.On:
                                        logging.info(f"Endpoint triggered for {app_name}")
                                        screenshot_path = self.capture_screenshot(app_name, active_window)
                                        if screenshot_path:
                                            logging.info(f"Screenshot saved: {screenshot_path}")
                                            # Add your trigger code here
                                            pass
                            except Exception as e:
                                logging.error(f"Error checking button state: {e}")
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()







-----2--2--2-2--2-2


import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()







 Let me break down the solution and explain its limitations and reliability.

The current approach has several potential issues that could affect reliability. Let me explain the core components and suggest a more robust approach.



<antArtifact identifier="robust-application-monitor" type="application/vnd.ant.code" language="python" title="Robust Application Monitor with UI Automation">
import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()
</antArtifact>

Let me explain why the previous approach wasn't 100% reliable and how this new version is more robust:

1. Key Improvements:

- Uses UI Automation instead of mouse position tracking
- Multiple button detection methods
- Window-specific screenshot capture
- Improved error handling and logging
- Caching for better performance
- More reliable event detection

2. Reliability Factors:

The previous version had these issues:
- Relied on mouse position (unreliable)
- Couldn't detect buttons accurately
- No fallback methods
- Vulnerable to window changes

The new version is more reliable because:

a) Button Detection:
   - Uses Windows UI Automation framework
   - Multiple search methods (Automation ID, Class Name, Name)
   - Doesn't rely on screen coordinates
   - Can detect actual UI elements

b) Window Handling:
   - Proper window detection
   - Caches window handles
   - Handles window state changes
   - Window-specific screenshots

c) Error Prevention:
   - Multiple fallback methods
   - Extensive error handling
   - Detailed logging
   - Resource cleanup

3. Limitations (Still Not 100%):

Even this improved version has some limitations:

- Some applications might block UI Automation
- Custom UI frameworks might not be fully compatible
- Window titles might change
- System performance could affect timing
- Some applications might use non-standard controls

4. To Make It More Reliable:

You should:

1. Add application-specific details:
```python
"OWS": {
    "window_pattern": "OWS",
    "endpoint": {
        "button_name": "OK",
        "automation_id": "okButton",  # Add actual ID
        "class_name": "Button"        # Add actual class
    }
}
```

2. Test with your specific applications and add:
   - Exact window titles
   - Button automation IDs
   - Class names
   - Any specific patterns

3. Add additional error handling for your environment

To use this version:

1. Install requirements:
```python
pip install uiautomation pillow pywin32
```

2. Run the script:
```python
python robust_monitor.py
```

Would you like me to:
1. Add support for specific applications you're using?
2. Implement additional detection methods?
3. Add specific error handling for your environment?
4. Add custom triggers for when endpoints are detected?




import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()






merhod-1 :

In this case, since the endpoint (like the "OK" button) doesn't exist in the window title, we'll need a way to detect specific buttons on the screen by using image recognition to look for UI elements. Here’s how we can achieve this with pyautogui to recognize on-screen buttons and trigger the screenshot capture when the final button (like "OK") is clicked.

Approach Outline

1. Image Recognition for Button Detection: Use pyautogui.locateOnScreen() to search for specific buttons on the screen by using screenshots of each button type as reference images. For example, take screenshots of the "Next," "Clear," and "OK" buttons and save them as images like next_button.png, clear_button.png, and ok_button.png.


2. Click Monitoring: Set up a listener with pynput to monitor each mouse click and use pyautogui.locateOnScreen() to check if the "OK" button is visible. When it's clicked, trigger the screenshot capture.


3. Trigger Screenshot on "OK" Click: Only take a screenshot when the "OK" button is clicked. We can determine this by using image matching right after a mouse click to check if the click position aligns with the "OK" button.



Here's an example of how this could be implemented in Python:

import time
import pyautogui
from pynput import mouse
from datetime import datetime

# Define button images for reference (these images should be screenshots of your buttons)
BUTTON_IMAGES = {
    "next": "next_button.png",   # Image of the "Next" button
    "clear": "clear_button.png", # Image of the "Clear" button
    "ok": "ok_button.png"        # Image of the "OK" button
}

# This flag helps track if we are in the final step (where "OK" appears)
awaiting_final_ok_click = False

def capture_screenshot():
    # Capture and save screenshot with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = f"screenshot_{timestamp}.png"
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    # Use pyautogui to search for the button on screen
    location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
    return location

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click
    
    # Only process on button release
    if not pressed:
        # Check for "OK" button visibility and click position
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location and ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width \
                    and ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                print("OK button clicked. Capturing screenshot.")
                capture_screenshot()
                awaiting_final_ok_click = False  # Reset the state after capturing screenshot
        else:
            # Check if "Next" button or other buttons are clicked, progressing to the final step
            next_button_location = check_button_visible("next")
            if next_button_location:
                print("Next button detected and clicked. Progressing workflow.")
                # Move forward in workflow, expecting OK button soon
                awaiting_final_ok_click = True

# Set up the mouse listener
with mouse.Listener(on_click=on_click) as listener:
    print("Monitoring started...")
    listener.join()

Explanation

1. Button Image Matching:

check_button_visible() checks if a specific button is on the screen by using an image of the button (e.g., next_button.png, clear_button.png, ok_button.png).

If locateOnScreen() finds the button with sufficient confidence, it returns the position of that button on the screen.



2. Mouse Click Monitoring:

In on_click, we first check if the "Next" or other intermediate buttons are visible and clicked.

If they are, we set awaiting_final_ok_click = True, indicating we’re ready for the "OK" button in the workflow.

When awaiting_final_ok_click is True, we listen for the "OK" button to be clicked at the specified location.



3. Screenshot Trigger on Final Click:

When awaiting_final_ok_click is True, the script will monitor for an "OK" button click.

Once detected, capture_screenshot() is called to save the current screen image.




Additional Notes

Button Images: Ensure the images of each button are clear and cropped tightly to the button's dimensions for accurate detection.

Confidence Parameter: The confidence parameter in locateOnScreen() allows for a match tolerance, helping with slight visual variations in buttons. Experiment with values like 0.8 or 0.9.

Error Handling: You might need to add error handling for cases where the buttons aren’t detected due to changes in UI.


This setup should help you track the click sequence, detect the final click on the "OK" button, and take a screenshot accordingly.




method-2: 


Given the need to monitor all types of applications, including both desktop and web, and to detect when a button click matches specific conditions (like the button being labeled "OK"), here’s a refined approach that combines accessibility APIs and click monitoring to address your requirements:

Approach Overview

1. Monitor Global Click Events: Use a global hook to detect mouse clicks.


2. Identify UI Elements at Click Locations: For each click, detect if it’s on a button or other UI control with specific attributes (such as “OK” or “Submit”) using accessibility APIs.


3. Trigger Screenshot on Matching Clicks: If the click matches the target endpoint (e.g., button labeled "OK"), trigger the screenshot capture.



Implementation Steps

1. Monitor Global Mouse Clicks

To globally monitor mouse clicks across applications, you can use libraries like pynput to set up a global mouse listener in Python. This listener will intercept all mouse events, regardless of the active application.

from pynput import mouse
import time

# Callback function for mouse clicks
def on_click(x, y, button, pressed):
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        # Call function to check if this click is on the desired button
        check_ui_element(x, y)

# Set up a global listener
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

This code captures each click's location (x, y coordinates). For every click, it calls check_ui_element(x, y) to inspect the UI element under the cursor.

2. Identify the Clicked UI Element

Using libraries like pywinauto or uiautomation, you can retrieve information about the UI element at the clicked position. Since pywinauto can inspect accessibility information for most applications, it’s helpful in identifying UI elements on both Windows and some web applications running in a browser.

Here’s an example using uiautomation to check if the click location has a button with the desired label:

from uiautomation import Control

def check_ui_element(x, y):
    # Find the control at the given screen coordinates
    control = Control(x=x, y=y)
    
    # Check if the control is a button and matches the desired title
    if control.ControlTypeName == 'Button' and control.Name in ['OK', 'Submit', 'Done']:
        print(f"Button '{control.Name}' clicked at ({x}, {y})")
        capture_screenshot()
    else:
        print("Click did not match the target button.")

This function:

Finds the control at the coordinates of the mouse click.

Checks the control type and name to see if it’s a button and if its name matches the desired endpoint (OK, Submit, Done).

Triggers a screenshot if the control meets these conditions.


3. Capture a Screenshot

To capture a screenshot, you can use libraries like Pillow. You’d call this function within check_ui_element() if the click matches your endpoint.

from PIL import ImageGrab
import datetime

def capture_screenshot():
    # Generate a timestamped filename
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

This code captures the entire screen and saves it with a timestamped filename when called.

Putting It All Together

Here’s a consolidated example combining all the pieces:

from pynput import mouse
from uiautomation import Control
from PIL import ImageGrab
import datetime

# Define a list of target button names
TARGET_BUTTON_NAMES = ['OK', 'Submit', 'Done']

# Function to capture a screenshot
def capture_screenshot():
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

# Function to check if the clicked element matches the target endpoint
def check_ui_element(x, y):
    control = Control(x=x, y=y)
    if control.ControlTypeName == 'Button' and control.Name in TARGET_BUTTON_NAMES:
        print(f"Button '{control.Name}' clicked at ({x}, {y})")
        capture_screenshot()
    else:
        print("Click did not match the target button.")

# Mouse click event handler
def on_click(x, y, button, pressed):
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        check_ui_element(x, y)

# Set up global mouse listener
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

Explanation of How It Works

1. Global Mouse Listener: The mouse.Listener detects all mouse clicks globally.


2. Element Inspection: For each click, check_ui_element(x, y) checks if the clicked UI element matches the target button criteria.


3. Screenshot Trigger: If a target button is clicked, capture_screenshot() is called to capture and save the screen.



Advantages of This Method

Versatile Application: Works across different types of applications since it relies on coordinates and UI Automation APIs.

Efficient Detection: Only triggers a screenshot for relevant button clicks, without constantly searching for button images.

Scalability: Easily adaptable if you need to monitor other UI elements or applications with similar button endpoints.


This approach avoids relying on visual matching and instead leverages UI properties, making it more robust for different types of applications.



method-3:

To streamline this process and avoid frequent image captures, you can use a more direct approach that combines system hooks and lightweight UI element checks. By using pyautogui to track specific clicks and uiautomation to verify elements only when a click occurs, you can avoid excessive image processing and reduce processing time. Here’s an optimized solution that focuses on capturing a screenshot only when the user clicks on the desired endpoint button (e.g., “OK”).

Optimized Solution Outline

1. Initial and Final Image Capture Only: Capture an initial image when the target window opens and a final image when the workflow endpoint is clicked (e.g., the “OK” button).


2. Direct Click Monitoring with pyautogui: Track clicks and verify the active window title to ensure you only check for clicks within relevant applications.


3. UI Automation for Click Validation: Instead of image processing for each click, use uiautomation to directly validate whether a click occurred on a specific button (like “OK”) within the target window.



Implementation Steps

This example captures a screenshot only when the “OK” button is clicked, marking the end of the workflow.

from pynput import mouse
import pyautogui
import time
import datetime
from PIL import ImageGrab
import uiautomation as automation
from ctypes import windll
import ctypes

# Target application and button configurations
TARGET_APPS = ["OWS", "AWS", "JWS"]
TARGET_BUTTON_NAMES = ["OK", "Submit", "Done"]

# Track start and end times
start_time = None

# Helper to get the current active window title
def get_active_window_title():
    hwnd = windll.user32.GetForegroundWindow()
    length = windll.user32.GetWindowTextLengthW(hwnd)
    buffer = ctypes.create_unicode_buffer(length + 1)
    windll.user32.GetWindowTextW(hwnd, buffer, length + 1)
    return buffer.value

# Capture screenshot function
def capture_screenshot():
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

# Function to verify button click
def check_if_button_clicked(x, y):
    control = automation.Control(x=x, y=y)
    if control.ControlTypeName == 'Button' and control.Name in TARGET_BUTTON_NAMES:
        return control.Name
    return None

# Event handler for mouse clicks
def on_click(x, y, button, pressed):
    global start_time
    if pressed:
        active_window = get_active_window_title()

        # Check if active window is one of the target applications
        if any(app in active_window for app in TARGET_APPS):
            # Track start time on first detected click in target app
            if start_time is None:
                start_time = datetime.datetime.now()
                print("Start time recorded:", start_time)

            # Check if the clicked element is a target button
            clicked_button = check_if_button_clicked(x, y)
            if clicked_button:
                print(f"'{clicked_button}' button clicked at ({x}, {y})")

                # Capture end time and screenshot if the endpoint is clicked
                if clicked_button in TARGET_BUTTON_NAMES:
                    end_time = datetime.datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"End of workflow detected. Duration: {duration} seconds.")
                    capture_screenshot()  # Capture final screenshot
                    start_time = None  # Reset for next workflow

# Start listening for mouse events
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

Explanation of the Optimized Solution

1. Window Title Check: Each click event first checks the active window title to ensure clicks are within the target application. This avoids unnecessary checks outside the relevant applications.


2. Direct Click Validation: When a click is detected, the code uses uiautomation to verify if it’s on a button matching the endpoint criteria (like "OK"). This avoids image comparisons, reducing processing time to milliseconds.


3. Minimal Screenshot Capture: Screenshots are only taken at the start (optional) and end of the workflow, ensuring no redundant image captures during the workflow.


4. Timing Control: The start_time and end_time are recorded only when the relevant window is active, and the workflow endpoint button is clicked, allowing for precise tracking of workflow duration.



Performance Considerations

Efficiency: By reducing image captures and relying on UI element checks only when needed, this method avoids the delays associated with frequent image processing.

Accuracy: Using uiautomation ensures that you’re validating clicks on actual UI elements (like buttons) rather than relying on pixel-based image matching.

Responsiveness: This approach minimizes overhead, so the response time should be close to milliseconds for each click check.


This setup should meet the requirement for fast, efficient tracking without the heavy processing of frequent image analysis.




Yes, you can print the UI tree using the uiautomation library, which allows you to visualize the hierarchy and properties of accessible UI elements. This can be very useful for understanding the structure of the application's UI and identifying the elements available for interaction.

Here’s how you can do it:

1. Install uiautomation (if you haven’t already):

pip install uiautomation


2. Use the automation.GetRootControl() Method to get the root of the UI tree, then recursively print the tree. This will print the properties of each accessible control, including control type and name.



Here’s an example code snippet that captures the UI tree and prints it:

import uiautomation as automation

def print_ui_tree(control, depth=0):
    """
    Recursively print the UI tree starting from the given control.
    """
    # Print the current control's information with indentation based on depth
    print("  " * depth + f"ControlType: {control.ControlTypeName}, Name: {control.Name}, Class: {control.ClassName}")

    # Recursively print child controls
    for child in control.GetChildren():
        print_ui_tree(child, depth + 1)

# Capture the root control (entire screen)
root_control = automation.GetRootControl()

# Print the UI tree from the root control
print_ui_tree(root_control)

Explanation

Root Control: automation.GetRootControl() gets the root control of the entire desktop UI. You can also use automation.GetFocusedControl() if you want to start from the currently active window.

Recursive Printing: The print_ui_tree function goes through each control's children and prints details, adjusting indentation (depth) for easier visualization.


Adjustments and Usage Tips

1. Limiting Depth: If the UI tree is very large, you might want to limit the depth to avoid printing too many levels.


2. Filtering Controls: You can add conditions within print_ui_tree to filter specific control types, names, or classes.


3. Focus on a Specific Window: If you only want to print the tree for a specific window, use automation.WindowControl(Name="Window Title") in place of GetRootControl().



Example Output

The output will look something like this:

ControlType: Window, Name: Untitled - Notepad, Class: Notepad
  ControlType: MenuBar, Name: , Class: 
    ControlType: MenuItem, Name: File, Class: 
    ControlType: MenuItem, Name: Edit, Class: 
  ControlType: Edit, Name: Text Editor, Class: 
  ControlType: StatusBar, Name: , Class:

This output helps you understand the hierarchy and properties of each control, which you can then use to identify and interact with specific elements based on ControlType, Name, or ClassName.




method-5:

Sure, here’s an example code snippet that follows this approach using pyautogui to capture a small area around the mouse click and pytesseract for OCR to detect if a particular button (e.g., an “OK” button) is in that region.

This code assumes you’re checking for a specific word, like “OK,” on the button to determine if it’s your endpoint. If the word is detected, it will then trigger a full-screen capture.

Code Example

import pyautogui
import pytesseract
from PIL import Image
import time

# Configure pytesseract path if needed
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def capture_and_check_button(click_x, click_y, region_size=(100, 50)):
    """
    Captures a small region around the click coordinates and checks for the target text.
    :param click_x: X-coordinate of the click.
    :param click_y: Y-coordinate of the click.
    :param region_size: Size of the region to capture around the click (width, height).
    :return: True if the target text is found, False otherwise.
    """
    # Define the region to capture around the click (x, y, width, height)
    region = (click_x - region_size[0] // 2, click_y - region_size[1] // 2, region_size[0], region_size[1])
    
    # Capture the region
    small_capture = pyautogui.screenshot(region=region)
    
    # Use OCR to read text from the captured region
    text = pytesseract.image_to_string(small_capture)
    print(f"OCR Text Detected: {text.strip()}")
    
    # Check if the target text (e.g., "OK") is present in the text
    return "OK" in text.strip()

def monitor_clicks(region_size=(100, 50)):
    """
    Monitors mouse clicks and captures the full screen if the target button is clicked.
    :param region_size: Size of the region to capture around each click.
    """
    print("Monitoring clicks... Press Ctrl+C to stop.")
    
    try:
        while True:
            # Wait for a mouse click
            if pyautogui.mouseDown(button="left"):
                # Get mouse position
                click_x, click_y = pyautogui.position()
                
                # Check if the small capture contains the endpoint button (e.g., "OK")
                if capture_and_check_button(click_x, click_y, region_size):
                    print("Endpoint button clicked! Capturing the full screen...")
                    
                    # Capture full screen if the endpoint button was clicked
                    full_screenshot = pyautogui.screenshot()
                    full_screenshot.save("full_screenshot.png")
                    print("Full screen captured.")
                
                # Allow a small delay to avoid multiple triggers from a single click
                time.sleep(0.5)
                
    except KeyboardInterrupt:
        print("Click monitoring stopped.")

# Start monitoring clicks
monitor_clicks()

Explanation

1. capture_and_check_button():

This function captures a small region around the click location (specified by click_x, click_y).

It uses OCR to read any text in the captured image.

If the OCR detects the word “OK” in the text, it returns True to indicate that the endpoint button is clicked.



2. monitor_clicks():

Continuously monitors for left mouse clicks.

For each click, it retrieves the mouse position and passes it to capture_and_check_button() to check for the endpoint button.

If capture_and_check_button() returns True, it captures the full screen and saves it as “full_screenshot.png”.




How to Adjust for Performance

Adjust region_size: Try different values to find the smallest region that reliably detects your button.

OCR Optimization: Since pytesseract can be slow, reduce OCR workload by:

Setting a specific OCR language or whitelist.

Considering alternatives like OpenCV template matching if button appearance is consistent.



This setup should process quickly for small regions, minimizing time compared to full-screen processing at each click.

