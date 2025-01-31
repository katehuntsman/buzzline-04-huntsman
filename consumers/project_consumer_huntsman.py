# project_consumer_huntsman.py

import json
import os
from collections import defaultdict
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

load_dotenv()

# Data structure to store sentiment by category
category_sentiment = defaultdict(lambda: {'total_sentiment': 0, 'message_count': 0})

# Set up live visuals
fig, ax = plt.subplots()
plt.ion()  # Interactive mode

def update_chart():
    """Update the live chart with the latest average sentiment per category."""
    # Clear the previous chart
    ax.clear()

    # Get categories and their average sentiment
    categories = list(category_sentiment.keys())
    average_sentiments = [data['total_sentiment'] / data['message_count'] for data in category_sentiment.values()]

    # Create the bar chart
    ax.bar(categories, average_sentiments, color="skyblue")

    # Set labels and title
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Sentiment")
    ax.set_title("Real-Time Average Sentiment by Category")

    # Rotate x-tick labels
    ax.set_xticklabels(categories, rotation=45, ha="right")

    # Adjust layout
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause for real-time update
    plt.pause(0.01)

def process_message(message: str) -> None:
    try:
        # Parse the message
        message_dict = json.loads(message)

        if isinstance(message_dict, dict):
            category = message_dict.get("category", "unknown")
            sentiment = message_dict.get("sentiment", 0)

            # Update category sentiment
            category_sentiment[category]['total_sentiment'] += sentiment
            category_sentiment[category]['message_count'] += 1

            # Update the chart with new data
            update_chart()

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    topic = os.getenv("PROJECT_TOPIC", "buzzline-topic")
    consumer = create_kafka_consumer(topic)

    logger.info(f"Polling messages from topic '{topic}'...")

    try:
        for message in consumer:
            message_str = message.value
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        plt.ioff()  # Turn off interactive mode
        plt.show()  # Show the final chart

if __name__ == "__main__":
    main()
