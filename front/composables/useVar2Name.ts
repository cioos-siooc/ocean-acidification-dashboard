export function var2name(varId: string): string {
    // Convert camelCase or snake_case to Title Case
    const withSpaces = varId
        .replace(/([a-z])([A-Z])/g, '$1 $2') // camelCase to spaces
        .replace(/_/g, ' '); // snake_case to spaces
    return withSpaces
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}