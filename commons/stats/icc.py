# Import libraries
import pandas as pd
import pingouin as pg


def _calculate_icc(hotkey_to_scores: dict[str, list[float]]) -> dict[str, float]:
    """Calculate ICC for each rater against the mean of all other raters

    Args:
        hotkey_to_scores (dict[str, list[float]]): Dictionary of miner hotkeys to their miner scores.
        NOTE: the ordering of miner scores should be handled outside this function!

    Returns:
        dict[str, float]: Dictionary of hotkeys to ICC scores
    """

    # Convert to DataFrame
    df = pd.DataFrame(hotkey_to_scores)

    # List of all raters, this would be hotkeys of miners
    raters = list(hotkey_to_scores.keys())

    # Dictionary to store ICC scores
    hotkey_to_icc2 = {}

    # Loop through each rater
    rater_key = "Rater"
    subject_key = "Subject"
    rating_key = "Rating"
    for rater in raters:
        # Calculate mean of all other raters (excluding the current rater)
        # we do this to ensure that the current rater is not compared to itself
        other_raters = [r for r in raters if r != rater]
        mean_key = f"Mean_excluding_{rater}"
        df[mean_key] = df[other_raters].mean(axis=1)

        # Subset data for the current rater and the mean of others
        pair_df = df[[rater, mean_key]]

        # Reshape to long format for ICC
        pair_long = pair_df.reset_index().melt(
            id_vars=["index"], var_name=rater_key, value_name=rating_key
        )
        pair_long.columns = [subject_key, rater_key, rating_key]

        # Calculate ICC (using ICC2 for two-way random effects, absolute agreement)
        # TODO: determine nan_policy & handle errors
        icc_result = pg.intraclass_corr(
            data=pair_long,
            targets=subject_key,
            raters=rater_key,
            ratings=rating_key,
        )

        # Extract ICC2 value
        icc_value = icc_result[icc_result["Type"] == "ICC2"]["ICC"].values[0]

        # Store the score
        hotkey_to_icc2[rater] = icc_value
    return hotkey_to_icc2


if __name__ == "__main__":
    # Sample data: 5 items rated by 4 raters
    # TODO: hotkey to scores, ensure proper ordering!!!
    data = {
        "Rater1": [7.5, 8.0, 6.5, 9.0, 7.0],
        "Rater2": [7.0, 8.5, 6.0, 8.5, 7.5],
        "Rater3": [8.0, 7.5, 6.8, 9.5, 6.5],
        "Rater4": [7.2, 8.2, 6.2, 9.2, 7.8],
    }
    icc_scores = _calculate_icc(data)
    # Display the results
    print("Inter-Rater Reliability (ICC) Scores:")

    for pair, score in icc_scores.items():
        print(f"{pair}: ICC = {score:.3f}")
