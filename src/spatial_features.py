def count_nearby_cafes(candidate, cafes, radius=500):
    buffer_cafe = candidate.buffer(radius)
    return cafes.geometry.intersects(buffer_cafe).sum()