def transform_data(data):
    print("Starting transformation...")
    transformed_data = []
    for item in data:
        transformed_item = {
            'id': item.get('id'),
            'name': item.get('name'),
            'category': item.get('category')
        }
        transformed_data.append(transformed_item)
    print("Transformation complete.")
    return transformed_data

def main():
    print("Transformation script started.")
    # Example data for testing
    example_data = [
        {'id': 1, 'name': 'Bus', 'category': 'Transport'},
        {'id': 2, 'name': 'Train', 'category': 'Transport'}
    ]
    transformed = transform_data(example_data)
    print("Transformed data:", transformed)

if __name__ == "__main__":
    main()